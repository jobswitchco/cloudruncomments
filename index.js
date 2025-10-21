const express = require("express");

const mongoose = require("mongoose");
const axios = require("axios");
const Automation = require("./models/Automation.js");
const RepliedComment = require("./models/RepliedComment.js");
const User = require("./models/User.js");        // 👈 add this




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
async function extractCommentEvents(envelope) {
  const events = [];

  const entries = envelope?.body?.entry || [];
  for (const entry of entries) {
    const entryTime = entry?.time || null;
    const changes = entry?.changes || [];

    for (const ch of changes) {
      const v = ch?.value || {};

      // In your payload: value.id = comment_id, value.media.id = media_id
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
        pageId: entry?.id, // IG business account ID
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

    console.log('I am kuthac chimpesstha');

    console.log('commentId-> : ', commentId);
    console.log('replyText-> : ', replyText);
    console.log('pageAccessToken-> : ', pageAccessToken);
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

  console.log('pubsub entered : ', commentEvents);

  for (const c of commentEvents) {
    console.log("💬 Comment received:", c.text);

    // 1️⃣ find matching automation(s)
    const automations = await Automation.find({
      platform: "instagram",
      postId: c.mediaId,
      status: "active",
    });

    console.log('MYDYYDYD Automations ::::::', automations);

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
