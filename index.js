import express from "express";
import mongoose from "mongoose";
import axios from "axios";
import qs from "qs";

import Automation from "./models/Automation.js";
import RepliedComment from "./models/RepliedComment.js";
import User from "./models/User.js";
import ActionLock from "./models/ActionLock.js";



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

const META_APP_ID = "1360956302356492";
const META_APP_SECRET = "2b21c578035bd7b96b24ba43e4479a52";

const FB_API = "https://graph.facebook.com/v24.0";
const DAY_MS = 24 * 60 * 60 * 1000;

function daysLeft(expiry) {
  if (!expiry) return -Infinity;
  return Math.floor((new Date(expiry).getTime() - Date.now()) / DAY_MS);
}

async function refreshFbTokensForUser(user) {
  // Re-exchange the current long-lived user token
  const llResp = await axios.get(`${FB_API}/oauth/access_token`, {
    params: {
      grant_type: "fb_exchange_token",
      client_id: process.env.META_APP_ID || META_APP_ID,
      client_secret: process.env.META_APP_SECRET || META_APP_SECRET,
      fb_exchange_token: user.fbLongLivedToken,
    },
  });

  const newUserLL = llResp.data?.access_token;
  if (!newUserLL) throw new Error("Failed to refresh long-lived user token");

  // If expires_in missing → set to 58 days
  const newUserExpiry = new Date(Date.now() + 58 * DAY_MS);

  // Re-fetch Page token using the fresh user token
  let newPageToken = user.fbPageAccessToken || null;
  if (user.fbPageId) {
    const pageTokResp = await axios.get(`${FB_API}/${user.fbPageId}`, {
      params: { fields: "access_token", access_token: newUserLL },
    });
    newPageToken = pageTokResp.data?.access_token || newPageToken;
  }

  await User.findByIdAndUpdate(user._id, {
    fbLongLivedToken: newUserLL,
    fbLongLivedTokenExpiry: newUserExpiry,
    fbPageAccessToken: newPageToken,
    fbLastRefreshAt: new Date(),
    fbNeedsReconnect: false,
    updated_at: new Date(),
  });

  return {
    fbPageAccessToken: newPageToken,
    fbPageId: user.fbPageId,
  };
}

/**
 * Ensure we have a fresh page token for a user.
 * - If fbLongLivedTokenExpiry < 28 days (or missing), refresh both tokens.
 * - Otherwise return existing tokens.
 */
async function ensureFreshPageTokenForUser(userId) {
  const user = await User.findById(userId)
    .select("_id instagramConnected fbLongLivedToken fbLongLivedTokenExpiry fbPageId fbPageAccessToken")
    .lean();

  if (!user || !user.instagramConnected) return { fbPageAccessToken: null, fbPageId: null };

  // If we don't even have a long-lived token, we can't proceed
  if (!user.fbLongLivedToken) return { fbPageAccessToken: null, fbPageId: user.fbPageId || null };

  const remain = daysLeft(user.fbLongLivedTokenExpiry);

  if (remain < 28) {
    try {
      return await refreshFbTokensForUser(user);
    } catch (e) {
      console.error("⚠️ FB refresh failed:", e?.response?.data || e.message || e);
      // Keep existing tokens if refresh fails; the Graph call may still work if not expired yet
      return {
        fbPageAccessToken: user.fbPageAccessToken || null,
        fbPageId: user.fbPageId || null,
      };
    }
  }

  // Still plenty of time
  return {
    fbPageAccessToken: user.fbPageAccessToken || null,
    fbPageId: user.fbPageId || null,
  };
}




// async function replyToCommentPublic(commentId, replyText, pageAccessToken) {
//   try {
//     console.log("Replying to comment:", commentId);
    
//     const url = `https://graph.facebook.com/v24.0/${commentId}/replies`;
//     const res = await axios.post(
//       url,
//       { message: replyText },
//       { 
//         params: { access_token: pageAccessToken }  // ✅ Use query param instead of header
//       }
//     );
    
//     console.log("✅ Replied to comment", commentId, res.data);
//     return res.data;
//   } catch (err) {
//     console.error(
//       "❌ IG reply failed",
//       commentId,
//       err.response?.data || err.message
//     );
//     throw err;
//   }
// }

async function replyToCommentPublic(commentId, replyText, pageAccessToken) {
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

// async function sendPrivateReply({ fbPageId, commentId, message, pageAccessToken }) {
 
//   const url = `https://graph.facebook.com/v24.0/${fbPageId}/messages`;
//   const { data, status } = await http.post(
//     url,
//     { recipient: { comment_id: String(commentId) }, message: { text: message } },
//     { params: { access_token: pageAccessToken } }
//   );

//     console.log("✅ Private message to comment", commentId);


//   if (status >= 400) {
//     const err = new Error("Private reply failed");
//     err.details = data?.error || data;
//     throw err;
//   }
//   return data;
// }

async function sendPrivateReply({ fbPageId, commentId, message, pageAccessToken, button }) {
  // One private reply per comment, within Meta window.
  // button: { text: 'Download', url: 'https://...' } or undefined

  const url = `https://graph.facebook.com/v24.0/${fbPageId}/messages`;

  // Build message body: use a button template when a valid URL/button exists
  const hasButton = button && typeof button.url === "string" && button.url.trim();

  const msgBody = hasButton
    ? {
        recipient: { comment_id: String(commentId) },
        message: {
          attachment: {
            type: "template",
            payload: {
              template_type: "button",
              text: message,
              buttons: [
                {
                  type: "web_url",
                  url: button.url,
                  title: button.text || "Open link",
                },
              ],
            },
          },
        },
      }
    : {
        recipient: { comment_id: String(commentId) },
        message: { text: message },
      };

  // Debug log — remove in production if needed
  console.log("-> sendPrivateReply payload:", JSON.stringify(msgBody));

  const { data, status } = await http.post(url, msgBody, {
    params: { access_token: pageAccessToken },
  });

  if (status >= 400) {
    const err = new Error("Private reply failed");
    err.details = data?.error || data;
    console.error("❌ sendPrivateReply error:", err.details || err.message);
    throw err;
  }

  console.log("✅ Private message to comment", commentId, data);
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

    const userTokenCache = new Map();

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
        // const user = await User.findById(auto.userId).lean();

         const key = String(auto.userId);
        let creds = userTokenCache.get(key);
        if (!creds) {
          creds = await ensureFreshPageTokenForUser(auto.userId);
          userTokenCache.set(key, creds);
        }

       const accessToken = creds.fbPageAccessToken;
        const fbPageId = creds.fbPageId;

        if (!accessToken || !fbPageId) {
          console.warn("⚠️ Missing fbPageAccessToken/fbPageId for user", key);
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
              const data = await replyToCommentPublic(
                              c.commentId,
                              auto.publicReply,
                              accessToken
                            );

              console.log('Why Reply Failing : ', data);
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
      button: auto.dm?.button, // pass the configured button (if any)
    });

    // 🔹 Fetch user details using their IGSID (c.fromUserId)
    const userDetailsUrl = `https://graph.facebook.com/v21.0/${c.fromUserId}`;
    const { data: userDetails } = await axios.get(userDetailsUrl, {
      params: {
        access_token: accessToken,
        fields:
          "id,username,profile_pic,is_user_follow_business,is_business_follow_user",
      },
    });

    privateSent = true;
    await finalizeAction({
      automationId: auto._id,
      commentId: c.commentId,
      channel: "private",
      ok: true,
    });

    // ✅ Store extended user info in RepliedComment
    await RepliedComment.create({
      commentId: c.commentId,
      automationId: auto._id,
      channel: "private",
      text: c.text,
      sentMessage: auto.dm.message,
      status: "sent",
      igUserId: userDetails.id, // IGSID
      username: userDetails.username,
      profilePic: userDetails.profile_pic,
      followsBusiness: userDetails.is_user_follow_business,
      businessFollowsUser: userDetails.is_business_follow_user,
    });

    console.log("✅ Private reply + user details saved", {
      commentId: c.commentId,
      user: userDetails,
    });
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
}

          
          
          
          else {
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
