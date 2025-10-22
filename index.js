import express from "express";
import mongoose from "mongoose";
import axios from "axios";
import Automation from "./models/Automation.js";
import RepliedComment from "./models/RepliedComment.js";
import User from "./models/User.js";
import qs from "qs";




const username = "jobswitchco";
const password = "1q2unIeMxwn9IpUB";
const MONGO_URI =
  "mongodb+srv://" +
  username +
  ":" +
  password +
  "@clusterjob.5grzhlw.mongodb.net/?retryWrites=true&w=majority&appName=ClusterJob";

const app = express();
app.use(express.json({ type: "*/*" }));




axios.interceptors.response.use(
  (r) => r,
  (e) => {
    const cfg = e.config || {};
    const urlWithQuery =
      cfg.url + (cfg.params ? `?${qs.stringify(cfg.params)}` : "");
    const body = e.response?.data || { message: e.message };
    console.error("[HTTP ERROR]", urlWithQuery, JSON.stringify(body, null, 2));
    return Promise.reject(e);
  }
);



// --- Mongo Connection ---
const connectMongo = async () => {
  if (mongoose.connection.readyState === 0) {
    await mongoose.connect(MONGO_URI, {
      maxPoolSize: 5,
      serverSelectionTimeoutMS: 10000,
    });
    console.log("✅ MongoDB connected");
  }
};

// --- Extract comment events ---
async function extractCommentEvents(envelope) {
  const events = [];

  const entries = envelope?.body?.entry || [];
  for (const entry of entries) {
    const entryTime = entry?.time || null;
    const changes = entry?.changes || [];

    for (const ch of changes) {
      const v = ch?.value || {};

      const commentId = v.id;
      const mediaId = v.media?.id;
      const text = v.text || "";
      const fromUserId = v.from?.id;
      const fromUsername = v.from?.username;

      events.push({
        eventId:
          envelope?.headers?.["X-Hub-Delivery"] ||
          commentId ||
          Math.random().toString(36),
        pageId: entry?.id,
        mediaId,
        commentId,
        text: text.toLowerCase(),
        fromUserId,
        fromUsername,
        timestamp: v.timestamp || entryTime || Date.now(),
      });
    }
  }

  return events;
}

// --- IG public reply helper ---
async function replyToComment(commentId, replyText, pageAccessToken) {
  try {
    console.log("Replying to comment:", commentId);
    
    const url = `https://graph.facebook.com/v24.0/${commentId}/replies`;
    const res = await axios.post(
      url,
      { message: replyText },
      { headers: { Authorization: `Bearer ${pageAccessToken}` } }
    );
    
    console.log("✅ Replied to comment", commentId, res.data);
    return res.data;
  } catch (err) {
    console.error(
      "❌ IG reply failed",
      commentId,
      err.response?.data || err.message
    );
    throw err;
  }
}


async function getIgUserIdForPage(pageId, pageAccessToken) {
  const url = `https://graph.facebook.com/v24.0/${pageId}`;
  const { data } = await axios.get(url, {
    params: {
      access_token: pageAccessToken,
      fields: "instagram_business_account{id,username}"
    }
  });
  const ig = data?.instagram_business_account;
  if (!ig?.id) throw new Error("No instagram_business_account linked to page");
  return ig.id; // <IG_USER_ID>
}

async function sendPrivateReply(igUserId, commentId, text, pageAccessToken) {
  if (!igUserId) throw new Error("IG user id missing");
  if (!commentId) throw new Error("commentId missing");
  if (!text || !text.trim()) throw new Error("message text missing");

  const url = `https://graph.facebook.com/v24.0/${igUserId}/messages`;
  const payload = {
    recipient: { comment_id: String(commentId) },
    message: { text: text.trim() }
  };

  try {
    const { data } = await axios.post(url, payload, {
      params: { access_token: pageAccessToken },
      headers: { "Content-Type": "application/json" },
      timeout: 15000
    });
    console.log("✅ Private Reply OK:", data);
    return { ok: true, data };
  } catch (err) {
    // The interceptor above will print the full details.
    return { ok: false, error: err.response?.data?.error || err.response?.data || { message: err.message } };
  }
}




// --- Pub/Sub push handler ---
app.post("/pubsub", async (req, res) => {
  const msg = req.body?.message;
  if (!msg || !msg.data) return res.status(204).send();

  let envelope;
  try {
    const json = Buffer.from(msg.data, "base64").toString("utf8");
    envelope = JSON.parse(json);
  } catch (e) {
    console.error("❌ Decode failed", e);
    return res.status(204).send();
  }

  await connectMongo();
  const commentEvents = await extractCommentEvents(envelope);

  console.log("pubsub entered:", commentEvents);

  for (const c of commentEvents) {
    console.log("💬 Comment received:", c.text);

    // 1️⃣ find matching automation(s)
    const automations = await Automation.find({
      platform: "instagram",
      postId: c.mediaId,
      status: "active",
    });

    console.log("Found automations:", automations.length);

    if (!automations?.length) {
      console.log("No active automation for post", c.mediaId);
      continue;
    }

    // 2️⃣ iterate each matching automation
    for (const auto of automations) {
      const matched = auto.keywords.some((kw) =>
        c.text.includes(kw.toLowerCase())
      );
      if (!matched) continue;

      // 3️⃣ skip if already replied
      const already = await RepliedComment.findOne({
        commentId: c.commentId,
        automationId: auto._id,
      });
      if (already) {
        console.log("Already replied to", c.commentId);
        continue;
      }

      // 4️⃣ get the user's access token from User collection
      const user = await User.findById(auto.userId);
      const accessToken = user?.fbPageAccessToken;
      const fbPageId = user?.fbPageId;
      if (!accessToken) {
        console.warn("⚠️ No access token found for user", auto.userId);
        continue;
      }

      let replySent = false;
      let dmSent = false;

      // 5️⃣ send public reply if configured
      if (auto.hasPublicReply && auto.publicReply) {
        try {
          await replyToComment(c.commentId, auto.publicReply, accessToken);
          replySent = true;
          console.log(
            `✅ Sent reply for keyword match "${auto.keywords.join(", ")}"`
          );
        } catch (err) {
          console.error("Reply failed", err.message);
        }
      }

      // 6️⃣ send DM if configured
    // 6️⃣ send DM/private reply if configured
if (auto.dm?.enabled && auto.dm?.message && c.fromUserId) {
  try {
    const igUserId = await getIgUserIdForPage(user.fbPageId, accessToken);
    console.log('igUserId::::::::::::', igUserId);
    // Use a Private Reply first (one-per-comment, within 7 days)
    const pr = await sendPrivateReply(igUserId, c.commentId, auto.dm.message, accessToken);

    if (pr.success) {
      dmSent = true; // count as a sent DM for your stats
    } else if (pr.code === 100 /* validation */) {
      // Possibly already used private reply for this comment or window elapsed
      console.warn("⚠️ Private reply not allowed for this comment now.");
    } else if (pr.code === 10 && pr.sub === 2534022) {
      // Shouldn't happen with private_replies, but keep for completeness
      console.warn("⚠️ Window issue.");
    }
  } catch (err) {
    console.error("DM/Private Reply failed", err.message);
  }
}


      // 7️⃣ record the interaction
      if (replySent || dmSent) {
        await RepliedComment.create({
          commentId: c.commentId,
          automationId: auto._id,
          text: c.text,
        });

        const updateStats = {
          $set: { "runStats.lastRunAt": new Date() },
        };
        
        if (replySent) {
          updateStats.$inc = { 
            ...updateStats.$inc, 
            "runStats.repliesSent": 1 
          };
        }
        
        if (dmSent) {
          updateStats.$inc = { 
            ...updateStats.$inc, 
            "runStats.dmsSent": 1 
          };
        }

        await Automation.updateOne({ _id: auto._id }, updateStats);
      }
    }
  }

  return res.status(204).send();
});

// --- Health check ---
app.get("/", (_req, res) => res.status(200).send("ok"));

const PORT = 8080;
app.listen(PORT, () => console.log(`🚀 Worker listening on ${PORT}`));