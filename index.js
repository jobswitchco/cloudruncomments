import express from "express";
import mongoose from "mongoose";
import axios from "axios";
import Automation from "./models/Automation.js";
import RepliedComment from "./models/RepliedComment.js";
import User from "./models/User.js";

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

// --- IG DM helper ---
async function sendInstagramDM(recipientUserId, message, pageId, pageAccessToken, button = null) {
  try {
    console.log("Sending DM to user:", recipientUserId);
    
    // Use the correct Instagram messaging endpoint with recipient as Instagram Scoped ID (IGSID)
    const url = `https://graph.facebook.com/v24.0/me/messages`;
    
    // Build message payload
    const payload = {
      recipient: { id: recipientUserId },
      message: { text: message }
    };

    // Add button if provided and message is text-only
    if (button && button.text && button.url) {
      // Instagram supports generic template with buttons
      payload.message = {
        attachment: {
          type: "template",
          payload: {
            template_type: "generic",
            elements: [
              {
                title: message.substring(0, 80), // Title has 80 char limit
                buttons: [
                  {
                    type: "web_url",
                    url: button.url,
                    title: button.text.substring(0, 20) // Button text has 20 char limit
                  }
                ]
              }
            ]
          }
        }
      };
    }

    const res = await axios.post(url, payload, {
      headers: { Authorization: `Bearer ${pageAccessToken}` },
      params: { access_token: pageAccessToken } // Some setups need this in params
    });

    console.log("✅ DM sent to user", recipientUserId, res.data);
    return { success: true, data: res.data };
  } catch (err) {
    const errorCode = err.response?.data?.error?.code;
    const errorMessage = err.response?.data?.error?.message || '';
    const errorSubcode = err.response?.data?.error?.error_subcode;
    
    // Handle Advanced Access permission error gracefully
    if (errorCode === 200 && errorMessage.includes('Advanced Access')) {
      console.warn(
        "⚠️ DM skipped - User not an app tester. Need Advanced Access approval.",
        recipientUserId
      );
      return { 
        success: false, 
        reason: 'awaiting_advanced_access',
        recipientId: recipientUserId 
      };
    }
    
    // Handle 24-hour window error
    if (errorCode === 10 && errorSubcode === 2534022) {
      console.warn(
        "⚠️ DM skipped - Outside 24-hour messaging window.",
        recipientUserId
      );
      return {
        success: false,
        reason: 'outside_messaging_window',
        recipientId: recipientUserId
      };
    }
    
    // Log and re-throw other errors
    console.error(
      "❌ IG DM failed",
      recipientUserId,
      err.response?.data || err.message
    );
    throw err;
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
      if (auto.dm?.enabled && auto.dm?.message && c.fromUserId) {
        try {
          const dmResult = await sendInstagramDM(
            c.fromUserId,
            auto.dm.message,
            fbPageId,
            accessToken,
            auto.dm.button
          );
          
          if (dmResult.success) {
            dmSent = true;
            console.log(`✅ Sent DM to user ${c.fromUsername || c.fromUserId}`);
          } else if (dmResult.reason === 'awaiting_advanced_access') {
            console.log(`⏳ DM pending Advanced Access approval for ${c.fromUsername || c.fromUserId}`);
            // Optionally: Store this in a queue for retry after approval
          }
        } catch (err) {
          console.error("DM failed with unexpected error", err.message);
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