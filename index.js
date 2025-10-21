import express from "express";
import mongoose from "mongoose";
import axios from "axios";
import Automation from "./models/Automation.js";
import RepliedComment from "./models/RepliedComment.js";

const username = 'jobswitchco';
const password = '1q2unIeMxwn9IpUB';

var MONGO_URI = 'mongodb+srv://'+username+':'+password+'@clusterjob.5grzhlw.mongodb.net/?retryWrites=true&w=majority&appName=ClusterJob';


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

// --- Utility: extract comment events ---
function extractCommentEvents(envelope) {
  const events = [];
  const entries = envelope?.body?.entry || [];
  for (const entry of entries) {
    const changes = entry?.changes || [];
    for (const ch of changes) {
      const v = ch?.value || {};
      if (v.comment_id || v.text || v.media_id) {
        events.push({
          eventId:
            envelope?.headers?.["X-Hub-Delivery"] ||
            v.id ||
            v.comment_id,
          pageId: entry?.id,
          mediaId: v.media_id || v.media?.id,
          commentId: v.comment_id,
          text: v.text?.toLowerCase() || "",
          fromUserId: v.from?.id,
          fromUsername: v.from?.username,
          timestamp: v.timestamp || v.time,
        });
      }
    }
  }
  return events;
}

// --- IG public reply helper ---
async function replyToComment(commentId, replyText, pageAccessToken) {
  try {
    const url = `https://graph.facebook.com/v20.0/${commentId}/replies`;
    const res = await axios.post(
      url,
      { message: replyText },
      { headers: { Authorization: `Bearer ${pageAccessToken}` } }
    );
    console.log("✅ Replied to comment", commentId, res.data);
    return res.data;
  } catch (err) {
    console.error("❌ IG reply failed", commentId, err.response?.data || err.message);
    throw err;
  }
}

// --- Pub/Sub push handler ---
app.post("/pubsub", async (req, res) => {
  const msg = req.body?.message;
  if (!msg || !msg.data) {
    console.warn("⚠️ No Pub/Sub message");
    return res.status(204).send();
  }

  let envelope;
  try {
    const json = Buffer.from(msg.data, "base64").toString("utf8");
    envelope = JSON.parse(json);
  } catch (e) {
    console.error("❌ Decode failed", e);
    return res.status(204).send();
  }

  await connectMongo();
  const commentEvents = extractCommentEvents(envelope);

  for (const c of commentEvents) {
    console.log("💬 Comment received:", c.text);

    // 1️⃣ find matching automation
    const automations = await Automation.find({
      platform: "instagram",
      postId: c.mediaId,
      status: "active",
    });

    if (!automations?.length) {
      console.log("No active automation for post", c.mediaId);
      continue;
    }

    // 2️⃣ check keywords
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

      // 4️⃣ post public reply (using stored access token per user)
      // you’ll need to store each user’s pageAccessToken in user doc or automation
      const accessToken = process.env.PAGE_ACCESS_TOKEN; // temp global token
      if (!accessToken) {
        console.warn("⚠️ No PAGE_ACCESS_TOKEN set");
        continue;
      }

      if (auto.hasPublicReply && auto.publicReply) {
        try {
          await replyToComment(c.commentId, auto.publicReply, accessToken);

          // 5️⃣ record reply
          await RepliedComment.create({
            commentId: c.commentId,
            automationId: auto._id,
            text: c.text,
          });

          // increment stats
          await Automation.updateOne(
            { _id: auto._id },
            {
              $inc: { "runStats.repliesSent": 1 },
              $set: { "runStats.lastRunAt": new Date() },
            }
          );

          console.log(
            `✅ Sent public reply for keyword match "${auto.keywords.join(", ")}"`
          );
        } catch (err) {
          console.error("Reply failed", err.message);
        }
      }
    }
  }

  return res.status(204).send();
});

// --- Health check ---
app.get("/", (_req, res) => res.status(200).send("ok"));

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`🚀 Worker listening on ${PORT}`));
