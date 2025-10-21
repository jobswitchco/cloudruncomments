import express from "express";
import mongoose from "mongoose";
import axios from "axios";
import Automation from "./models/Automation.js";
import RepliedComment from "./models/RepliedComment.js";
import User from "./models/User.js";

const app = express();
app.use(express.json({ type: "*/*" }));

const MONGO_URI = "mongodb+srv://jobswitchco:1q2unIeMxwn9IpUB@clusterjob.5grzhlw.mongodb.net/?retryWrites=true&w=majority&appName=ClusterJob";

const connectMongo = async () => {
  if (mongoose.connection.readyState === 0) {
    await mongoose.connect(MONGO_URI, {
      maxPoolSize: 5,
      serverSelectionTimeoutMS: 10000,
    });
    console.log("✅ MongoDB connected");
  }
};

async function extractCommentEvents(envelope) {
  const events = [];
  const entries = envelope?.body?.entry || [];
  for (const entry of entries) {
    const entryTime = entry?.time || null;
    const changes = entry?.changes || [];
    for (const ch of changes) {
      const v = ch?.value || {};
      events.push({
        eventId:
          envelope?.headers?.["X-Hub-Delivery"] || v.id || Math.random().toString(36),
        pageId: entry?.id,
        mediaId: v.media?.id,
        commentId: v.id,
        text: v.text?.toLowerCase() || "",
        fromUserId: v.from?.id,
        fromUsername: v.from?.username,
        timestamp: v.timestamp || entryTime || Date.now(),
      });
    }
  }
  return events;
}

// --- Public reply helper ---
async function replyToComment(commentId, replyText, pageAccessToken) {
  try {
    const url = `https://graph.facebook.com/v24.0/${commentId}/replies`;
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

// --- DM helper ---
async function sendInstagramDM(pageId, userId, message, pageAccessToken) {
  try {
    const url = `https://graph.facebook.com/v20.0/${pageId}/messages`;
    const payload = { recipient: { id: userId }, message: { text: message } };
    const res = await axios.post(url, payload, {
      headers: { Authorization: `Bearer ${pageAccessToken}` },
    });
    console.log(`📩 Sent DM to user ${userId}`, res.data);
    return res.data;
  } catch (err) {
    console.error("❌ DM send failed", userId, err.response?.data || err.message);
  }
}

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

  for (const c of commentEvents) {
    const automations = await Automation.find({
      platform: "instagram",
      postId: c.mediaId,
      status: "active",
    });

    for (const auto of automations) {
      const matched = auto.keywords.some((kw) => c.text.includes(kw.toLowerCase()));
      if (!matched) continue;

      const already = await RepliedComment.findOne({
        commentId: c.commentId,
        automationId: auto._id,
      });
      if (already) continue;

      const user = await User.findById(auto.userId);
      const accessToken = user?.fbPageAccessToken;
      if (!accessToken) continue;

      if (auto.hasPublicReply && auto.publicReply) {
        await replyToComment(c.commentId, auto.publicReply, accessToken);

        await RepliedComment.create({
          commentId: c.commentId,
          automationId: auto._id,
          text: c.text,
        });

        await Automation.updateOne(
          { _id: auto._id },
          { $inc: { "runStats.repliesSent": 1 }, $set: { "runStats.lastRunAt": new Date() } }
        );

        // 📨 Send DM if enabled
        if (auto.dm?.enabled && auto.dm?.message && c.fromUserId) {
          await sendInstagramDM(c.pageId, c.fromUserId, auto.dm.message, accessToken);
          await Automation.updateOne(
            { _id: auto._id },
            { $inc: { "runStats.dmsSent": 1 } }
          );
        }
      }
    }
  }

  res.status(204).send();
});

app.get("/", (_req, res) => res.status(200).send("ok"));
const PORT = 8080;
app.listen(PORT, () => console.log(`🚀 Worker listening on ${PORT}`));
