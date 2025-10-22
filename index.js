import express from "express";
import mongoose from "mongoose";
import axios from "axios";
import qs from "qs";

import Automation from "./models/Automation.js";
import RepliedComment from "./models/RepliedComment.js";
import User from "./models/User.js";
import ActionLock from "./models/ActionLock.js";

// const {
//   MONGO_URI = "",
//   PORT = 8080,
//   NODE_ENV = "production",
//   // Optionally validate Pub/Sub push with a shared secret:
//   PUBSUB_TOKEN = "", // if you set one, require it via header in /pubsub
// } = process.env;

// if (!MONGO_URI) {
//   console.error("❌ MONGO_URI env var is required");
//   process.exit(1);
// }


const username = "jobswitchco";
const password = "1q2unIeMxwn9IpUB";
const MONGO_URI =
  "mongodb+srv://" +
  username +
  ":" +
  password +
  "@clusterjob.5grzhlw.mongodb.net/?retryWrites=true&w=majority&appName=ClusterJob";

const PORT=8080;
const PUBSUB_TOKEN = "";

const app = express();
app.use(express.json({ type: "*/*" }));

// ---------- Axios setup ----------
const http = axios.create({
  timeout: 15000,
  // We prefer query param access_token for Graph
  // but allow header bearer in helpers if needed
  validateStatus: (s) => s >= 200 && s < 500, // let us inspect 4xx in code
});

http.interceptors.response.use(
  (r) => r,
  (e) => {
    const cfg = e.config || {};
    const urlWithQuery = cfg.url + (cfg.params ? `?${qs.stringify(cfg.params)}` : "");
    const body = e.response?.data || { message: e.message };
    console.error("[HTTP ERROR]", urlWithQuery, JSON.stringify(body, null, 2));
    return Promise.reject(e);
  }
);

// ---------- Mongo ----------
async function connectMongo() {
  if (mongoose.connection.readyState === 0) {
    await mongoose.connect(MONGO_URI, {
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 10000,
    });
    console.log("✅ MongoDB connected");
  }
}

// ---------- Utils ----------
const nowIso = () => new Date().toISOString();

// Normalize keywords/comments for robust matching
function normalize(str = "") {
  return String(str).toLowerCase().trim();
}

// Extract Instagram comment events (kept similar to your version)
async function extractCommentEvents(envelope) {
  const events = [];
  const entries = envelope?.body?.entry || [];

  for (const entry of entries) {
    const entryTime = entry?.time || null;
    const changes = entry?.changes || [];

    for (const ch of changes) {
      const v = ch?.value || {};
      // Only process comments
      // (optionally gate by field/type if your webhook sends many types)
      const isComment = !!v?.text && !!v?.id;
      if (!isComment) continue;

      events.push({
        eventId: envelope?.headers?.["X-Hub-Delivery"] || v.id || Math.random().toString(36),
        pageId: entry?.id,
        mediaId: v.media?.id,
        commentId: v.id,
        text: normalize(v.text),
        fromUserId: v.from?.id,
        fromUsername: v.from?.username,
        timestamp: v.timestamp || entryTime || Date.now(),
      });
    }
  }
  return events;
}

// ---------- Graph helpers ----------
async function replyToCommentPublic({ commentId, message, pageAccessToken }) {
  const url = `https://graph.facebook.com/v24.0/${commentId}/replies`;
  const { data, status } = await http.post(
    url,
    { message },
    { params: { access_token: pageAccessToken } }
  );

  if (status >= 400) {
    const err = new Error("Public reply failed");
    err.details = data?.error || data;
    throw err;
  }
  return data;
}

async function sendPrivateReply({ fbPageId, commentId, message, pageAccessToken }) {
  // One private reply per comment, within Meta window.
  const url = `https://graph.facebook.com/v24.0/${fbPageId}/messages`;
  const { data, status } = await http.post(
    url,
    { recipient: { comment_id: String(commentId) }, message: { text: message } },
    { params: { access_token: pageAccessToken } }
  );

  if (status >= 400) {
    const err = new Error("Private reply failed");
    err.details = data?.error || data;
    throw err;
  }
  return data;
}

// Reserve an action atomically (idempotency gate)
async function reserveAction({ automationId, commentId, channel }) {
  try {
    const doc = await ActionLock.findOneAndUpdate(
      { automationId, commentId, channel },
      {
        $setOnInsert: {
          state: "reserved",
          reservedAt: new Date(),
        },
      },
      { upsert: true, new: true }
    ).lean();

    // If brand-new: we just reserved it — proceed.
    // If existing and not "reserved", someone already handled it — skip.
    if (doc.state !== "reserved" || !doc.reservedAt) return { proceed: false, doc };
    return { proceed: true, doc };
  } catch (e) {
    // Duplicate key means someone reserved it milliseconds before us => skip
    if (e?.code === 11000) return { proceed: false, error: e };
    throw e;
  }
}

// Mark the reserved action outcome
async function finalizeAction({ automationId, commentId, channel, ok, error }) {
  const update = ok
    ? { state: "sent", sentAt: new Date(), error: undefined }
    : { state: "failed", error };

  await ActionLock.updateOne(
    { automationId, commentId, channel },
    { $set: update }
  );
}

// ---------- Pub/Sub handler ----------
app.post("/pubsub", async (req, res) => {
  try {
    if (PUBSUB_TOKEN) {
      const headerToken = req.get("X-Pubsub-Token");
      if (headerToken !== PUBSUB_TOKEN) {
        console.warn("⚠️ Unauthorized Pub/Sub push");
        return res.status(401).send("unauthorized");
      }
    }

    const msg = req.body?.message;
    if (!msg?.data) return res.status(204).send();

    let envelope;
    try {
      const json = Buffer.from(msg.data, "base64").toString("utf8");
      envelope = JSON.parse(json);
    } catch (e) {
      console.error("❌ Pub/Sub decode failed", e);
      return res.status(204).send();
    }

    await connectMongo();
    const commentEvents = await extractCommentEvents(envelope);
    if (!commentEvents.length) return res.status(204).send();

    for (const c of commentEvents) {
      // 1) Find matching automation(s)
      const autos = await Automation.find({
        platform: "instagram",
        status: "active",
        postId: c.mediaId,
      }).lean();

      if (!autos.length) {
        console.log("ℹ️ No automation for media", c.mediaId);
        continue;
      }

      for (const auto of autos) {
        const normalizedKeywords = auto.keywords.map(normalize).filter(Boolean);
        const matched = normalizedKeywords.some((kw) => c.text.includes(kw));
        if (!matched) continue;

        // 2) Load user tokens
        const user = await User.findById(auto.userId).lean();
        const accessToken = user?.fbPageAccessToken;
        const fbPageId = user?.fbPageId;
        if (!accessToken || !fbPageId) {
          console.warn("⚠️ Missing fbPageAccessToken/fbPageId for user", String(auto.userId));
          continue;
        }

        let publicSent = false;
        let privateSent = false;

        // ---- PUBLIC REPLY (idempotent) ----
        if (auto.hasPublicReply && auto.publicReply) {
          const { proceed } = await reserveAction({
            automationId: auto._id,
            commentId: c.commentId,
            channel: "public",
          });

          if (proceed) {
            try {
              const data = await replyToCommentPublic({
                commentId: c.commentId,
                message: auto.publicReply,
                pageAccessToken: accessToken,
              });
              publicSent = true;
              await finalizeAction({
                automationId: auto._id,
                commentId: c.commentId,
                channel: "public",
                ok: true,
              });
              await RepliedComment.create({
                commentId: c.commentId,
                automationId: auto._id,
                channel: "public",
                text: c.text,
                status: "sent",
              });
              console.log("✅ Public reply sent", { commentId: c.commentId, data });
            } catch (err) {
              console.error("❌ Public reply failed", err.details || err.message);
              await finalizeAction({
                automationId: auto._id,
                commentId: c.commentId,
                channel: "public",
                ok: false,
                error: err.details || { message: err.message },
              });
              await RepliedComment.create({
                commentId: c.commentId,
                automationId: auto._id,
                channel: "public",
                text: c.text,
                status: "failed",
                error: err.details || { message: err.message },
              });
            }
          } else {
            console.log("↩️ Skipping public (already reserved/sent)", c.commentId);
          }
        }

        // ---- PRIVATE REPLY (idempotent) ----
        if (auto.dm?.enabled && auto.dm?.message && c.fromUserId) {
          const { proceed } = await reserveAction({
            automationId: auto._id,
            commentId: c.commentId,
            channel: "private",
          });

          if (proceed) {
            try {
              const data = await sendPrivateReply({
                fbPageId,
                commentId: c.commentId,
                message: auto.dm.message,
                pageAccessToken: accessToken,
              });
              privateSent = true;
              await finalizeAction({
                automationId: auto._id,
                commentId: c.commentId,
                channel: "private",
                ok: true,
              });
              await RepliedComment.create({
                commentId: c.commentId,
                automationId: auto._id,
                channel: "private",
                text: c.text,
                status: "sent",
              });
              console.log("✅ Private reply sent", { commentId: c.commentId, data });
            } catch (err) {
              console.error("❌ Private reply failed", err.details || err.message);
              await finalizeAction({
                automationId: auto._id,
                commentId: c.commentId,
                channel: "private",
                ok: false,
                error: err.details || { message: err.message },
              });
              await RepliedComment.create({
                commentId: c.commentId,
                automationId: auto._id,
                channel: "private",
                text: c.text,
                status: "failed",
                error: err.details || { message: err.message },
              });
            }
          } else {
            console.log("↩️ Skipping private (already reserved/sent)", c.commentId);
          }
        }

        // ---- Update automation counters if anything went out ----
        if (publicSent || privateSent) {
          const inc = {};
          if (publicSent) inc["runStats.repliesSent"] = 1;
          if (privateSent) inc["runStats.dmsSent"] = 1;

          await Automation.updateOne(
            { _id: auto._id },
            {
              $set: { "runStats.lastRunAt": new Date() },
              ...(Object.keys(inc).length ? { $inc: inc } : {}),
            }
          );
        }
      }
    }

    // Pub/Sub ACK
    return res.status(204).send();
  } catch (err) {
    // Returning non-2xx forces Pub/Sub to retry; only do this for transient, top-level failures.
    console.error("❌ Unhandled /pubsub error", err.message);
    return res.status(500).send("error");
  }
});

// ---------- Health ----------
app.get("/", (_req, res) => res.status(200).send("ok"));

// ---------- Start ----------
app.listen(PORT, () => {
  console.log(`🚀 Worker listening on ${PORT} at ${new Date().toISOString()}`);
});
