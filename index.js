const express = require("express");

const mongoose = require("mongoose");
const axios = require("axios");
const Automation = require("./models/Automation.js");
const RepliedComment = require("./models/RepliedComment.js");
const  User = require("./models/User.js");        // 👈 add this




// Accept Pub/Sub push JSON (it posts with content-type: application/json)

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
function extractCommentEvents(envelope) {
  const events = [];

  const entries = envelope?.body?.entry || [];
  for (const entry of entries) {
    const changes = entry?.changes || [];
    for (const ch of changes) {
      const v = ch?.value || {};

      // Defensive fallback: some payloads have nested structures
      const commentId =
        v.comment_id ||
        v.id ||
        v.comment?.id ||
        v.media?.comment_id ||
        v.media_comment_id ||
        v.parent_id;

      const mediaId =
        v.media_id ||
        v.media?.id ||
        v.photo_id ||
        v.video_id ||
        v.post_id;

      const timestamp =
        v.timestamp ||
        v.time ||
        v.created_time ||
        v.comment_time ||
        null;

      const text =
        v.text ||
        v.message ||
        v.body ||
        v.caption ||
        "";

      events.push({
        eventId:
          envelope?.headers?.["X-Hub-Delivery"] || commentId || Math.random().toString(36),
        pageId: entry?.id,
        mediaId,
        commentId,
        text: text.toLowerCase(),
        fromUserId: v.from?.id || v.username || v.user_id,
        fromUsername: v.from?.username || v.username || "",
        timestamp,
      });
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
    console.error(
      "❌ IG reply failed",
      commentId,
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
  const commentEvents = extractCommentEvents(envelope);

  for (const c of commentEvents) {
    console.log("💬 Comment received:", c.text);

    // 1️⃣ find matching automation(s)
    const automations = await Automation.find({
      platform: "instagram",
      postId: c.mediaId,
      status: "active",
    });

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
      if (!accessToken) {
        console.warn("⚠️ No access token found for user", auto.userId);
        continue;
      }

      // 5️⃣ send public reply if configured
      if (auto.hasPublicReply && auto.publicReply) {
        try {
          await replyToComment(c.commentId, auto.publicReply, accessToken);

          await RepliedComment.create({
            commentId: c.commentId,
            automationId: auto._id,
            text: c.text,
          });

          await Automation.updateOne(
            { _id: auto._id },
            {
              $inc: { "runStats.repliesSent": 1 },
              $set: { "runStats.lastRunAt": new Date() },
            }
          );

          console.log(
            `✅ Sent reply for keyword match "${auto.keywords.join(", ")}"`
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
