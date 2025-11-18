// index.js for cloudruncomments service - PUBSUB PROCESSOR ONLY
import express from "express";
import mongoose from "mongoose";
import axios from "axios";
import qs from "qs";
import crypto from "crypto";
import Automation from "./models/Automation.js";
import RepliedComment from "./models/RepliedComment.js";
import User from "./models/User.js";
import ActionLock from "./models/ActionLock.js";
import ConversationState from "./models/ConversationState.js";

const app = express();
app.use(express.json({ type: "*/*" }));

// Config
const PORT = 8080;
const PUBSUB_TOKEN = process.env.PUBSUB_TOKEN || "";
const META_APP_ID = "1360956302356492";
const META_APP_SECRET = "2b21c578035bd7b96b24ba43e4479a52";
const FB_API = "https://graph.facebook.com/v24.0";
const DAY_MS = 24 * 60 * 60 * 1000;

const MONGO_URI = "mongodb+srv://jobswitchco:1q2unIeMxwn9IpUB@clusterjob.5grzhlw.mongodb.net/?retryWrites=true&w=majority&appName=ClusterJob";

// ---------- Axios setup ----------
const http = axios.create({
  timeout: 15000,
  validateStatus: (s) => s >= 200 && s < 500,
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

// ---------- MongoDB ----------
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
function normalize(str = "") {
  return String(str).toLowerCase().trim();
}

async function extractCommentEvents(envelope) {
  const events = [];
  const entries = envelope?.body?.entry || [];
  
  for (const entry of entries) {
    const entryTime = entry?.time || null;
    const changes = entry?.changes || [];

    for (const ch of changes) {
      const v = ch?.value || {};
      events.push({
        eventId: envelope?.headers?.["X-Hub-Delivery"] || v.id || Math.random().toString(36),
        pageId: entry?.id,
        mediaId: v.media?.id,
        commentId: v.id,
        text: (v.text || "").toLowerCase(),
        fromUserId: v.from?.id,
        fromUsername: v.from?.username,
        timestamp: v.timestamp || entryTime || Date.now(),
      });
    }
  }
  return events;
}

function daysLeft(expiry) {
  if (!expiry) return -Infinity;
  return Math.floor((new Date(expiry).getTime() - Date.now()) / DAY_MS);
}

async function refreshFbTokensForUser(user) {
  const llResp = await axios.get(`${FB_API}/oauth/access_token`, {
    params: {
      grant_type: "fb_exchange_token",
      client_id: META_APP_ID,
      client_secret: META_APP_SECRET,
      fb_exchange_token: user.fbLongLivedToken,
    },
  });

  const newUserLL = llResp.data?.access_token;
  if (!newUserLL) throw new Error("Failed to refresh long-lived user token");

  const newUserExpiry = new Date(Date.now() + 58 * DAY_MS);

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

async function ensureFreshPageTokenForUser(userId) {
  const user = await User.findById(userId)
    .select("_id instagramConnected fbLongLivedToken fbLongLivedTokenExpiry fbPageId fbPageAccessToken")
    .lean();

  if (!user || !user.instagramConnected) return { fbPageAccessToken: null, fbPageId: null };
  if (!user.fbLongLivedToken) return { fbPageAccessToken: null, fbPageId: user.fbPageId || null };

  const remain = daysLeft(user.fbLongLivedTokenExpiry);

  if (remain < 28) {
    try {
      return await refreshFbTokensForUser(user);
    } catch (e) {
      console.error("⚠️ FB refresh failed:", e?.response?.data || e.message);
      return {
        fbPageAccessToken: user.fbPageAccessToken || null,
        fbPageId: user.fbPageId || null,
      };
    }
  }

  return {
    fbPageAccessToken: user.fbPageAccessToken || null,
    fbPageId: user.fbPageId || null,
  };
}

// ---------- Message Sending Functions ----------
async function replyToCommentPublic(commentId, replyText, pageAccessToken) {
  const url = `${FB_API}/${commentId}/replies`;
  const res = await axios.post(
    url,
    { message: replyText },
    { headers: { Authorization: `Bearer ${pageAccessToken}` } }
  );
  console.log("✅ Replied to comment", commentId);
  return res.data;
}

async function sendTextMessage({ fbPageId, commentId, message, pageAccessToken }) {
  const url = `${FB_API}/${fbPageId}/messages`;
  const msgBody = {
    recipient: { comment_id: String(commentId) },
    message: { text: message },
  };

  const { data, status } = await http.post(url, msgBody, {
    params: { access_token: pageAccessToken },
  });

  if (status >= 400) {
    const err = new Error("Text message failed");
    err.details = data?.error || data;
    throw err;
  }

  console.log("✅ Text message sent", commentId);
  return data;
}

async function sendButtonTemplate({ fbPageId, commentId, message, buttons, pageAccessToken }) {
  if (!Array.isArray(buttons) || buttons.length === 0) {
    throw new Error("buttons array is required");
  }

  const cleanButtons = buttons
    .filter(Boolean)
    .slice(0, 3)
    .map((btn) => {
      const url = typeof btn.url === "string" ? btn.url.trim() : "";
      const title = String(btn.text || btn.title || "Open").trim().slice(0, 20);
      return { original: btn, url, title };
    })
    .filter((b) => b.url && b.url.startsWith("https://"));

  if (!cleanButtons.length) {
    throw new Error("No valid buttons after normalization (require https URLs, max 3)");
  }

  const payloadButtons = cleanButtons.map((b) => ({
    type: "web_url",
    url: b.url,
    title: b.title || "Open",
  }));

  const url = `${FB_API}/${fbPageId}/messages`;
  const msgBody = {
    recipient: { comment_id: String(commentId) },
    message: {
      attachment: {
        type: "template",
        payload: {
          template_type: "button",
          text: message || "",
          buttons: payloadButtons,
        },
      },
    },
  };

  try {
    const { data, status } = await http.post(url, msgBody, {
      params: { access_token: pageAccessToken },
    });

    if (status >= 400) {
      const err = new Error("Button template failed");
      err.details = data?.error || data;
      throw err;
    }

    console.log("✅ Button template sent", { commentId, response: data });
    return { ok: true, data };
  } catch (err) {
    const details = err.details || err.response?.data || (err.message ? { message: err.message } : err);
    console.error("⚠️ sendButtonTemplate error:", JSON.stringify(details, null, 2));

    const subcode = details?.error_subcode || details?.error?.error_subcode || details?.code;
    if (subcode === 2534023 || (details?.message && /already has a reply/i.test(details.message))) {
      const noteworthy = new Error("Comment already has a reply (2534023)");
      noteworthy.details = details;
      noteworthy.code = 2534023;
      throw noteworthy;
    }

    const e = new Error("Button template failed");
    e.details = details;
    throw e;
  }
}

async function sendPrivateReply({ fbPageId, commentId, message, pageAccessToken, button }) {
  const hasButton = button && typeof button.url === "string" && button.url.trim();

  if (!hasButton) {
    return await sendTextMessage({ fbPageId, commentId, message, pageAccessToken });
  }

  try {
    const resp = await sendButtonTemplate({ fbPageId, commentId, message, buttons: [button], pageAccessToken });
    return resp;
  } catch (err) {
    if (err.code === 2534023 || (err.details && err.details.error_subcode === 2534023)) {
      console.warn("⚠️ Button template rejected (already has reply). Attempting text fallback.");
      const fallbackMsg = message ? `${message}\n\nOpen here: ${button.url}` : `Open here: ${button.url}`;

      try {
        const txt = await sendTextMessage({ fbPageId, commentId, message: fallbackMsg, pageAccessToken });
        console.log("✅ Fallback text succeeded");
        return { ok: true, data: txt, fallback: true, reason: "already_has_reply" };
      } catch (tErr) {
        console.error("❌ Fallback text failed:", JSON.stringify(tErr?.response?.data || tErr?.message));
        const re = new Error("Button and fallback text both failed");
        re.details = { buttonError: err.details, fallbackError: tErr?.response?.data || tErr?.message };
        throw re;
      }
    }

    console.warn("⚠️ sendButtonTemplate failed. Trying text fallback.");
    const fallbackMsg = message ? `${message}\n\nOpen here: ${button.url}` : `Open here: ${button.url}`;
    try {
      const txt = await sendTextMessage({ fbPageId, commentId, message: fallbackMsg, pageAccessToken });
      console.log("✅ Fallback text succeeded");
      return { ok: true, data: txt, fallback: true, reason: "button_error" };
    } catch (tErr) {
      console.error("❌ Fallback text failed:", JSON.stringify(tErr?.response?.data || tErr?.message));
      const re = new Error("Button and fallback text both failed");
      re.details = { buttonError: err.details, fallbackError: tErr?.response?.data || tErr?.message };
      throw re;
    }
  }
}


async function reserveAction({ automationId, postId, igUserId, commentText, commentId, channel }) {
  const now = new Date();
  const textHash = crypto.createHash('md5').update(commentText || '').digest('hex');
  
  try {
    // Attempt to create a NEW lock document with state "reserved"
    // This will FAIL if a document already exists (due to unique index)
    const newLock = await ActionLock.create({
      automationId,
      postId,
      igUserId,
      textHash,
      commentId,
      channel,
      state: "reserved",
      reservedAt: now,
    });

    console.log(`✅ Lock acquired for ${channel}:`, commentId);
    return { proceed: true };
    
  } catch (err) {
    // Duplicate key error (E11000) means lock already exists
    if (err.code === 11000) {
      console.log(`ℹ️ Action already processed for ${channel}:`, commentId);
      return { proceed: false };
    }
    
    // Other errors should be logged and block the action
    console.error("❌ reserveAction error:", err.message);
    return { proceed: false };
  }
}

async function finalizeAction({ automationId, postId, igUserId, commentText, commentId, channel, ok, error }) {
  const textHash = crypto.createHash('md5').update(commentText || '').digest('hex');
  
  try {
    const update = {
      state: ok ? "sent" : "failed",
      sentAt: ok ? new Date() : null,
      error: error || null,
    };

    const result = await ActionLock.updateOne(
      { 
        automationId, 
        postId, 
        igUserId, 
        textHash, 
        channel, 
        commentId,
        state: "reserved" // Only update if still in reserved state
      },
      { $set: update }
    );

    if (result.matchedCount === 0) {
      console.warn(`⚠️ No reserved lock found to finalize for ${channel}:`, commentId);
    } else {
      console.log(`✅ Lock finalized for ${channel}:`, commentId, ok ? "SUCCESS" : "FAILED");
    }
    
  } catch (err) {
    console.error("❌ finalizeAction error:", err.message);
  }
}

// ============================================================================
// OPTIONAL: Cleanup function to remove old failed/expired reservations
// ============================================================================
async function cleanupStaleLocks() {
  const staleThreshold = new Date(Date.now() - 60 * 60 * 1000); // 1 hour ago
  
  try {
    const result = await ActionLock.deleteMany({
      state: "reserved",
      reservedAt: { $lt: staleThreshold }
    });
    
    if (result.deletedCount > 0) {
      console.log(`🧹 Cleaned up ${result.deletedCount} stale locks`);
    }
  } catch (err) {
    console.error("❌ cleanupStaleLocks error:", err.message);
  }
}

async function isEventProcessed(eventId) {
  if (!eventId) return false;
  
  try {
    const existing = await ProcessedEvent.findOne({ eventId });
    return !!existing;
  } catch (err) {
    console.error("❌ Error checking processed event:", err.message);
    return false; // Fail open to avoid blocking legitimate events
  }
}

async function markEventProcessed(eventId) {
  if (!eventId) return;
  
  try {
    await ProcessedEvent.create({ eventId });
    console.log("✅ Event marked as processed:", eventId);
  } catch (err) {
    if (err.code === 11000) {
      console.log("ℹ️ Event already marked as processed:", eventId);
    } else {
      console.error("❌ Error marking event:", err.message);
    }
  }
}


// ---------- CRITICAL: sendFlowMessage - handles BOTH comment_id (private reply) and user id (DM) ----------
async function sendFlowMessage({ recipient, flowNode, pageAccessToken, fbPageId }) {
  const { type, message, quick_replies, buttons, cards, media_url } = flowNode || {};

  if (!recipient || (!recipient.comment_id && !recipient.id)) {
    throw new Error("recipient (comment_id or id) is required");
  }

  if (!fbPageId) {
    throw new Error("fbPageId is required for messaging endpoint");
  }

  // Determine recipient based on what's available and message type
  let sendRecipient;
  let useCommentId = false;

  // Quick replies CANNOT be sent via comment_id (private reply)
  // They require user id (DM with 24hr window)
  if (type === "quick_replies") {
    if (!recipient.id) {
      throw new Error("Quick replies require recipient.id (cannot be sent as private reply)");
    }
    sendRecipient = { id: String(recipient.id) };
    console.log(`→ Sending quick_replies to user ID:`, recipient.id);
  } else {
    // For text, button, generic, media: prefer comment_id if available (private reply)
    if (recipient.comment_id) {
      sendRecipient = { comment_id: String(recipient.comment_id) };
      useCommentId = true;
      console.log(`→ Sending ${type} to comment ID:`, recipient.comment_id);
    } else if (recipient.id) {
      sendRecipient = { id: String(recipient.id) };
      console.log(`→ Sending ${type} to user ID:`, recipient.id);
    } else {
      throw new Error("Either comment_id or id must be provided");
    }
  }

  const url = `${FB_API}/${fbPageId}/messages`;

  console.log("→ sendFlowMessage", {
    type,
    recipient: sendRecipient,
    useCommentId,
  });

  const doPost = async (body) => {
    const { data, status } = await http.post(url, body, { params: { access_token: pageAccessToken } });
    if (status >= 400) {
      const err = new Error(`${type} message failed`);
      err.details = data?.error || data;
      throw err;
    }
    return data;
  };

  switch (type) {
    case "text": {
      const body = { recipient: sendRecipient, message: { text: message || "" } };
      return await doPost(body);
    }

    case "quick_replies": {
      if (!quick_replies || quick_replies.length === 0) {
        throw new Error("quick_replies array is required");
      }
      const qrs = quick_replies
        .slice(0, 13)
        .map((qr, idx) => ({
          content_type: "text",
          title: (qr.title || "").toString().slice(0, 20),
          payload: qr.payload || `QR_${Date.now()}_${idx}`,
        }));

      const body = {
        recipient: sendRecipient,
        message: {
          text: message || "",
          quick_replies: qrs,
        },
      };

      return await doPost(body);
    }

    case "button": {
      if (!buttons || buttons.length === 0) {
        throw new Error("buttons array is required");
      }
      const payloadButtons = buttons
        .slice(0, 3)
        .map((b) => ({
          type: "web_url",
          url: (b.url || b.link || "").toString(),
          title: (b.text || b.title || "Open").toString().slice(0, 20),
        }));

      const body = {
        recipient: sendRecipient,
        message: {
          attachment: {
            type: "template",
            payload: {
              template_type: "button",
              text: message || "",
              buttons: payloadButtons,
            },
          },
        },
      };

      return await doPost(body);
    }

    case "generic": {
      if (!cards || cards.length === 0) {
        throw new Error("cards array is required");
      }

      const elements = cards.slice(0, 10).map((card) => ({
        title: card.title,
        subtitle: card.subtitle || undefined,
        image_url: card.image_url || undefined,
        buttons: card.button
          ? [
              {
                type: "web_url",
                url: card.button.url,
                title: (card.button.text || "Open").toString().slice(0, 20),
              },
            ]
          : undefined,
      }));

      const body = {
        recipient: sendRecipient,
        message: {
          attachment: {
            type: "template",
            payload: {
              template_type: "generic",
              elements,
            },
          },
        },
      };

      return await doPost(body);
    }

    case "media": {
      if (!media_url) {
        throw new Error("media_url required");
      }
      const body = {
        recipient: sendRecipient,
        message: {
          attachment: {
            type: "image",
            payload: { url: media_url },
          },
        },
      };
      return await doPost(body);
    }

    default:
      throw new Error(`Unknown flow node type: ${type}`);
  }
}



async function handleQuickReply(event) {
  console.log("➡️ Quick reply received");
  await handlePostback(event);
}

async function handleTextMessage(event) {
  const senderId = event.sender?.id;
  const text = event.message?.text;
  console.log("💬 Text message:", { senderId, text });
  
  // Check if user is responding to initial private reply
  const conversation = await ConversationState.findOne({
    igUserId: senderId,
    status: "active",
    currentFlowId: "awaiting_user_response",
    expiresAt: { $gt: new Date() },
  }).sort({ startedAt: -1 });
  
  if (conversation) {
    console.log("✅ User responded to initial message, 24hr window now open");
    
    const creds = await ensureFreshPageTokenForUser(conversation.userId);
    const accessToken = creds.fbPageAccessToken;
    const fbPageId = creds.fbPageId;
    
    const initialNode = conversation.flowConfig.initial;
    
    try {
      // Now we can send quick replies using user ID
      await sendFlowMessage({
        recipient: { id: String(senderId) },
        flowNode: initialNode,
        pageAccessToken: accessToken,
        fbPageId: fbPageId,
      });
      
      conversation.currentFlowId = "initial";
      conversation.addHistory({
        flowId: "initial",
        flowName: "INITIAL",
        messageSent: initialNode.message,
        userReply: text,
      });
      await conversation.save();
      
      console.log("✅ Quick replies sent after user text response");
    } catch (err) {
      console.error("❌ Failed to send quick replies:", err.message);
      conversation.markError(err);
      await conversation.save();
    }
  }
}

// ========== PUB/SUB ENDPOINT: COMMENTS ==========
app.post("/pubsub", async (req, res) => {
  try {
    console.log("📨 /pubsub called (comments)");

    if (PUBSUB_TOKEN) {
      const headerToken = req.get("X-Pubsub-Token");
      if (headerToken !== PUBSUB_TOKEN) {
        console.warn("⚠️ Unauthorized");
        return res.status(401).send("unauthorized");
      }
    }

    const msg = req.body?.message;
    if (!msg?.data) {
      console.log("ℹ️ Empty message");
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

    const commentEvents = await extractCommentEvents(envelope);
    if (!commentEvents.length) {
      console.log("ℹ️ No comment events");
      return res.status(204).send();
    }

    console.log(`📬 Processing ${commentEvents.length} comment(s)`);

    const userTokenCache = new Map();

for (const c of commentEvents) {
  const automations = await Automation.find({
    platform: "instagram",
    status: "active",
    postId: c.mediaId,
  }).lean();

  if (!automations.length) {
    console.log("ℹ️ No active automation for media:", c.mediaId);
    continue;
  }

  for (const auto of automations) {
    // Keyword matching
    const normalizedKeywords = (auto.keywords || []).map(normalize).filter(Boolean);
    const matched = normalizedKeywords.length === 0 || normalizedKeywords.some((kw) => c.text.includes(kw));
    if (!matched) {
      console.log("ℹ️ No keyword match for comment:", c.text);
      continue;
    }

    // Fetch tokens (with caching)
    let creds = userTokenCache.get(String(auto.userId));
    if (!creds) {
      creds = await ensureFreshPageTokenForUser(auto.userId);
      userTokenCache.set(String(auto.userId), creds);
    }

    const { fbPageAccessToken: accessToken, fbPageId } = creds;
    if (!accessToken || !fbPageId) {
      console.warn("⚠️ Missing tokens for user:", auto.userId);
      continue;
    }

    // ========== PUBLIC REPLY ==========
    if (auto.hasReply && auto.replyComment) {
      const { proceed } = await reserveAction({
        automationId: auto._id,
        postId: c.mediaId,
        igUserId: c.fromUserId,
        commentText: c.text,
        commentId: c.commentId,
        channel: "public",
      });

      if (proceed) {
        try {
          await replyToCommentPublic(c.commentId, auto.replyComment, accessToken);
          console.log("✅ Public reply sent:", c.commentId);

          await finalizeAction({
            automationId: auto._id,
            postId: c.mediaId,
            igUserId: c.fromUserId,
            commentText: c.text,
            commentId: c.commentId,
            channel: "public",
            ok: true,
          });
        } catch (err) {
          console.error("❌ Public reply failed:", err.message);
          
          await finalizeAction({
            automationId: auto._id,
            postId: c.mediaId,
            igUserId: c.fromUserId,
            commentText: c.text,
            commentId: c.commentId,
            channel: "public",
            ok: false,
            error: { message: err.message },
          });
          
          continue; // Skip private DM if public reply failed
        }
      } else {
        console.log("ℹ️ Public reply already sent for:", c.commentId);
      }
    }

    // ========== PRIVATE DM ==========
    if (auto.dmMessage && auto.buttonText) {
      const { proceed: canSendPrivate } = await reserveAction({
        automationId: auto._id,
        postId: c.mediaId,
        igUserId: c.fromUserId,
        commentText: c.text,
        commentId: c.commentId,
        channel: "private",
      });

      if (canSendPrivate) {
        try {
          // Send initial DM
          await sendInitialDM({
            fbPageId,
            commentId: c.commentId,
            automation: auto,
            pageAccessToken: accessToken,
            igUserId: c.fromUserId,
            igUsername: c.fromUsername,
          });

          // Fetch user details
          const userDetailsUrl = `${FB_API}/${c.fromUserId}`;
          const { data: userDetails } = await axios.get(userDetailsUrl, {
            params: {
              access_token: accessToken,
              fields: "id,username,profile_pic,is_user_follow_business,is_business_follow_user",
            },
          });

          // Save to RepliedComment
          await RepliedComment.create({
            commentId: c.commentId,
            postId: c.mediaId,
            automationId: auto._id,
            channel: "private",
            state: "sent",
            text: c.text,
            sentMessage: auto.dmMessage,
            status: "pending",
            igUserId: userDetails.id,
            username: userDetails.username,
            profilePic: userDetails.profile_pic,
            followsBusiness: userDetails.is_user_follow_business,
            businessFollowsUser: userDetails.is_business_follow_user,
          });

          await finalizeAction({
            automationId: auto._id,
            postId: c.mediaId,
            igUserId: c.fromUserId,
            commentText: c.text,
            commentId: c.commentId,
            channel: "private",
            ok: true,
          });

          console.log("✅ Private DM sent:", c.commentId);
          
        } catch (err) {
          console.error("❌ Private DM failed:", err.message);
          
          await finalizeAction({
            automationId: auto._id,
            postId: c.mediaId,
            igUserId: c.fromUserId,
            commentText: c.text,
            commentId: c.commentId,
            channel: "private",
            ok: false,
            error: { message: err.message },
          });
        }
      } else {
        console.log("ℹ️ Private DM already sent for:", c.commentId);
      }
    }
  }
}

    return res.status(204).send();
  } catch (err) {
    console.error("❌ /pubsub error", err.message, err.stack);
    return res.status(500).send("error");
  }
});




async function sendInitialDM({
  fbPageId,
  commentId,
  automation,
  pageAccessToken,
  igUserId,
  igUsername,
}) {
  try {
    // ONLY send button DM, nothing else
    const buttonPayload = {
      type: "postback",
      title: automation.buttonText,
      payload: `FLOW_START_${automation._id}`,
    };

    const url = `${FB_API}/${fbPageId}/messages`;
    const buttonBody = {
      recipient: { comment_id: String(commentId) },
      message: {
        attachment: {
          type: "template",
          payload: {
            template_type: "button",
            text: automation.dmMessage,
            buttons: [buttonPayload],
          },
        },
      },
    };

    const { data: btnData, status: btnStatus } = await http.post(
      url,
      buttonBody,
      { params: { access_token: pageAccessToken } }
    );

    if (btnStatus >= 400) {
      throw new Error(`Button send failed: ${JSON.stringify(btnData)}`);
    }

    console.log("✅ Initial DM button sent");

    // Create ConversationState
    const firstNode = automation.flowNodes?.[0];
    
    await ConversationState.create({
      userId: automation.userId,
      automationId: automation._id,
      commentId: commentId,
      igUserId: igUserId,
      igUsername: igUsername,
       currentFlowId: String(automation.flowNodes[0]?.id),
      flowConfig: automation.flowNodes,
      conversationHistory: [
        {
          flowId: String(firstNode?.id),
          flowName: "INITIAL",
          messageSent: automation.dmMessage,
          timestamp: new Date(),
        },
      ],
      status: "active",
      startedAt: new Date(),
      expiresAt: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
    });

    console.log("✅ ConversationState created");

    return { ok: true };
  } catch (err) {
    console.error("❌ Error in sendInitialDM:", err.message);
    throw err;
  }
}


async function sendFlowNodeMessage({
  fbPageId,
  igUserId,
  pageAccessToken,
  flowNode,
}) {
  if (!flowNode) {
    throw new Error("flowNode is required");
  }

  const url = `${FB_API}/${fbPageId}/messages`;

  // If node is quickReply type, send quick_replies
  if (flowNode.type === "quickReply") {
    const quickReplies = (flowNode.replyOptions || [])
      .slice(0, 13)
      .map((option) => ({
        content_type: "text",
        title: (option.text || "Option").toString().slice(0, 20),
        payload: `QR_${flowNode.id}_${option.id}`,
      }));

    if (quickReplies.length === 0) {
      throw new Error("No quick reply options available");
    }

    const qrBody = {
      recipient: { id: String(igUserId) },  // ✅ Use user id for quick replies
      message: {
        text: flowNode.config?.quickReplyQuestion || "Choose one:",
        quick_replies: quickReplies,
      },
    };

    console.log("→ Sending quick_replies to user:", igUserId);

    const { data: qrData, status: qrStatus } = await http.post(
      url,
      qrBody,
      { params: { access_token: pageAccessToken } }
    );

    if (qrStatus >= 400) {
      throw new Error(`Quick replies failed: ${JSON.stringify(qrData)}`);
    }

    console.log("✅ Quick replies sent");
    return qrData;
  }

  // If other node types, handle differently
  throw new Error(`Unsupported node type: ${flowNode.type}`);
}

// ========== PUB/SUB ENDPOINT: MESSAGING ==========
app.post("/pubsub-messaging", async (req, res) => {
  try {
    console.log("📨 /pubsub-messaging called");

    if (PUBSUB_TOKEN) {
      const headerToken = req.get("X-Pubsub-Token");
      if (headerToken !== PUBSUB_TOKEN) {
        console.warn("⚠️ Unauthorized");
        return res.status(401).send("unauthorized");
      }
    }

    const msg = req.body?.message;
    if (!msg?.data) {
      console.log("ℹ️ Empty message");
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

    const entries = envelope?.body?.entry || [];
    if (!entries.length) {
      console.log("ℹ️ No entries");
      return res.status(204).send();
    }

    for (const entry of entries) {
      const messaging = entry?.messaging || [];

      console.log(`📬 Processing ${messaging.length} messaging events`);

      for (const event of messaging) {
        // ✅ UPDATED: Handle postback events
        if (event.postback) {
          await handlePostback(event);
          continue;
        }

        // ✅ UPDATED: Handle quick_reply events (same as postback)
        if (event.message?.quick_reply) {
          await handlePostback(event);
          continue;
        }

        // ✅ UPDATED: Handle regular text messages
        if (event.message && !event.message.quick_reply) {
          await handleTextMessage(event);
          continue;
        }

        // Handle reactions
        if (event.reaction) {
          console.log("👍 Reaction:", event.reaction);
          continue;
        }
      }
    }

    return res.status(204).send();
  } catch (err) {
    console.error("❌ /pubsub-messaging error", err.message, err.stack);
    return res.status(500).send("error");
  }
});

// ============================================================================
// NEW HELPER FUNCTION: Send Quick Replies for Flow Node
// ============================================================================
// Call this function when you need to send quick_replies from a flow node

async function sendQuickRepliesForNode({
  fbPageId,
  senderId,
  flowNode,
  pageAccessToken,
}) {
  if (!flowNode || flowNode.type !== "quickReply") {
    throw new Error("flowNode must be of type 'quickReply'");
  }

  if (!flowNode.config || !flowNode.config.quickReplyQuestion) {
    throw new Error("Quick reply node missing question");
  }

  if (!flowNode.replyOptions || flowNode.replyOptions.length === 0) {
    throw new Error("Quick reply node missing reply options");
  }

  const url = `${FB_API}/${fbPageId}/messages`;

  // Build quick reply options from flowNode.replyOptions
  const quickReplies = flowNode.replyOptions
    .slice(0, 13) // Max 13 quick replies
    .map((option, idx) => ({
      content_type: "text",
      title: (option.text || `Option ${idx + 1}`).toString().slice(0, 20),
      payload: option.id ? `QR_${flowNode.id}_${option.id}` : `QR_${flowNode.id}_${idx}`,
    }));

  if (quickReplies.length === 0) {
    throw new Error("No valid quick reply options after processing");
  }

  const messageBody = {
    recipient: { id: String(senderId) },
    message: {
      text: flowNode.config.quickReplyQuestion || "Choose an option:",
      quick_replies: quickReplies,
    },
  };

  console.log("→ Sending quick replies:", {
    senderId,
    question: flowNode.config.quickReplyQuestion,
    optionCount: quickReplies.length,
  });

  const { data, status } = await http.post(url, messageBody, {
    params: { access_token: pageAccessToken },
  });

  if (status >= 400) {
    const err = new Error("Quick replies send failed");
    err.details = data?.error || data;
    throw err;
  }

  console.log("✅ Quick replies sent successfully");
  return data;
}



// ============================================================================
// UPDATED: Dynamic Flow Execution Engine
// ============================================================================

/**
 * Execute a flow node dynamically based on its type
 * This handles: quickReply, followCheck, and any future node types
 */
async function executeFlowNode({
  flowNode,
  conversation,
  senderId,
  pageAccessToken,
  fbPageId,
  selectedOption = null, // For quick reply selections
}) {
  console.log(`→ Executing flow node type: ${flowNode.type}, id: ${flowNode.id}`);

  switch (flowNode.type) {
    case "quickReply":
      return await executeQuickReplyNode({
        flowNode,
        conversation,
        senderId,
        pageAccessToken,
        fbPageId,
      });

    case "followCheck":
      return await executeFollowCheckNode({
        flowNode,
        conversation,
        senderId,
        pageAccessToken,
        fbPageId,
      });

    default:
      throw new Error(`Unknown node type: ${flowNode.type}`);
  }
}

/**
 * Execute a quickReply node
 */
async function executeQuickReplyNode({
  flowNode,
  conversation,
  senderId,
  pageAccessToken,
  fbPageId,
}) {

  console.log('flowNode : ', flowNode);
  console.log('conversation : ', conversation);
  console.log('senderId : ', senderId);
  console.log('pageAccessToken : ', pageAccessToken);
  console.log('fbPageId : ', fbPageId);
  
  const quickReplies = (flowNode.replyOptions || [])
    .slice(0, 13)
    .map((option) => ({
      content_type: "text",
      title: (option.text || "Option").toString().slice(0, 20),
      payload: `QR_${flowNode.id}_${option.id}`,
    }));

  if (quickReplies.length === 0) {
    throw new Error("No quick reply options available");
  }

  const url = `${FB_API}/${fbPageId}/messages`;
  const qrBody = {
    recipient: { id: String(senderId) },
    message: {
      text: flowNode.config?.quickReplyQuestion || "Choose one:",
      quick_replies: quickReplies,
    },
  };

  console.log("→ Sending quick_replies to user:", senderId);

  const { data: qrData, status: qrStatus } = await http.post(url, qrBody, {
    params: { access_token: pageAccessToken },
  });

  if (qrStatus >= 400) {
    throw new Error(`Quick replies failed: ${JSON.stringify(qrData)}`);
  }

  try {
  const response = await http.post(url, qrBody, {
    params: { access_token: pageAccessToken },
  });
  if (response.status >= 400) {
    console.error("Facebook API error data:", response.data);
    throw new Error(`Quick replies failed: ${JSON.stringify(response.data)}`);
  }
  return response.data;
} catch (err) {
  if (err.response) {
    console.error("FB API response error status:", err.response.status);
    console.error("FB API response error data:", err.response.data);
  } else {
    console.error("Error in HTTP request:", err.message);
  }
  throw err;
}


  console.log("✅ Quick replies sent");

  // Update conversation
  conversation.addHistory({
    flowId: String(flowNode.id),
    flowName: "QUICK_REPLY",
    messageSent: flowNode.config?.quickReplyQuestion,
    timestamp: new Date(),
  });

  conversation.currentFlowId = String(flowNode.id);
  await conversation.save();

  return { success: true };
}

/**
 * Execute a followCheck node
 */
async function executeFollowCheckNode({
  flowNode,
  conversation,
  senderId,
  pageAccessToken,
  fbPageId,
}) {
  console.log("→ Checking user follow status");

  // Fetch user follow status
  const userDetailsUrl = `${FB_API}/${senderId}`;
  let userDetails;
  try {
    const response = await axios.get(userDetailsUrl, {
      params: {
        access_token: pageAccessToken,
        fields: "id,username,profile_pic,is_user_follow_business,is_business_follow_user",
      },
    });
    userDetails = response.data;
  } catch (err) {
    console.error("❌ Failed to fetch user follow status:", err.message);
    userDetails = { is_user_follow_business: false };
  }

  const isFollowing = userDetails.is_user_follow_business === true;

  console.log("🔍 Follow Status:", { userId: senderId, isFollowing });

  if (isFollowing) {
    // User is following - send success message and button
    const followingButtons = flowNode.followingButtons || [];

    if (followingButtons.length === 0) {
      console.warn("⚠️ No following buttons configured");
      return { success: true, completed: true };
    }

    const followingButton = followingButtons[0];

    if (followingButton.actions && followingButton.actions.length > 0) {
      const action = followingButton.actions[0];

      await sendFlowMessage({
        recipient: { id: senderId },
        flowNode: {
          type: "button",
          message: flowNode.config.followCheckYesMessage,
          buttons: [
            {
              type: "web_url",
              title: followingButton.text,
              url: action.config?.redirectUrl || "https://example.com",
            },
          ],
        },
        pageAccessToken,
        fbPageId,
      });
    } else {
      await sendFlowMessage({
        recipient: { id: senderId },
        flowNode: {
          type: "text",
          message: flowNode.config.followCheckYesMessage,
        },
        pageAccessToken,
        fbPageId,
      });
    }

    conversation.addHistory({
      flowId: String(flowNode.id),
      flowName: "FOLLOW_CHECK_SUCCESS",
      messageSent: flowNode.config.followCheckYesMessage,
      userReply: "Following verified",
    });

    conversation.currentFlowId = String(flowNode.id);
    await conversation.save();

    console.log("✅ FollowCheck completed - user is following");
    return { success: true, completed: true };
  } else {
    // User not following - send verification button
    const notFollowingButtons = flowNode.notFollowingButtons || [];
    const verificationButton = notFollowingButtons[0];

    if (verificationButton) {
      const verificationPayload = `FOLLOWCHECK_RECHECK_${flowNode.id}`;

      await sendFlowMessage({
        recipient: { id: senderId },
        flowNode: {
          type: "button",
          message: flowNode.config.followCheckNoMessage,
          buttons: [
            {
              type: "postback",
              title: verificationButton.text,
              payload: verificationPayload,
            },
          ],
        },
        pageAccessToken,
        fbPageId,
      });

      conversation.addHistory({
        flowId: String(flowNode.id),
        flowName: "FOLLOW_CHECK_RETRY",
        messageSent: flowNode.config.followCheckNoMessage,
        timestamp: new Date(),
      });

      conversation.currentFlowId = String(flowNode.id);
      await conversation.save();

      console.log("✅ FollowCheck verification button sent");
    }

    return { success: true, awaitingRetry: true };
  }
}

/**
 * Execute an action (redirectLink, nested quickReply, etc.)
 */
async function executeAction({
  action,
  selectedOption,
  conversation,
  senderId,
  pageAccessToken,
  fbPageId,
  parentNodeId,
}) {
  console.log(`→ Executing action type: ${action.type}`);

  switch (action.type) {
    case "redirectLink": {
      const redirectUrl = action.config?.redirectUrl || "https://example.com";

      console.log("→ Sending redirect link:", redirectUrl);

      await sendFlowMessage({
        recipient: { id: senderId },
        flowNode: {
          type: "button",
          message: `You selected: ${selectedOption.text} ✓`,
          buttons: [
            {
              type: "web_url",
              title: "Open Link",
              url: redirectUrl,
            },
          ],
        },
        pageAccessToken,
        fbPageId,
      });

      console.log("✅ Redirect link sent");
      return { success: true, completed: true };
    }

    case "quickReply": {
      // ✅ NESTED QUICK REPLY
      console.log("→ Executing nested quickReply");

      const nestedConfig = action.config;

      if (!nestedConfig || !nestedConfig.quickReplyQuestion) {
        console.warn("⚠️ Nested quick reply config missing");
        return { success: false, error: "Missing nested config" };
      }

      const nestedOptions = nestedConfig.replyOptions || [];
      const quickReplies = nestedOptions.slice(0, 13).map((option) => ({
        content_type: "text",
        title: (option.text || "Option").toString().slice(0, 20),
        payload: `QR_NESTED_${parentNodeId}_${option.id}`,
      }));

      if (quickReplies.length === 0) {
        console.warn("⚠️ No nested quick reply options");
        return { success: false, error: "No options" };
      }

      await sendFlowMessage({
        recipient: { id: senderId },
        flowNode: {
          type: "quick_replies",
          message: nestedConfig.quickReplyQuestion || "Choose one:",
          quick_replies: quickReplies,
        },
        pageAccessToken,
        fbPageId,
      });

      console.log("✅ Nested quick replies sent");

      // Store nested config for next interaction
      conversation.currentNestedQuickReplyConfig = {
        parentNodeId: String(parentNodeId),
        parentOptionId: String(selectedOption.id),
        nestedConfig: nestedConfig,
      };

      await conversation.save();

      return { success: true, isNested: true };
    }

    default:
      console.warn(`⚠️ Unknown action type: ${action.type}`);
      return { success: false, error: "Unknown action type" };
  }
}

/**
 * Find the next node in the flow configuration
 * This traverses the flowNodes array to find what comes after the current node
 */
function getNextFlowNode(flowConfig, currentNodeId) {
  const currentIndex = flowConfig.findIndex(
    (node) => String(node.id) === String(currentNodeId)
  );

  if (currentIndex === -1) {
    console.warn("⚠️ Current node not found in flow config");
    return null;
  }

  // Check if there's a next node in the array
  if (currentIndex + 1 < flowConfig.length) {
    return flowConfig[currentIndex + 1];
  }

  // No more nodes - flow is complete
  return null;
}

// ============================================================================
// UPDATED: handlePostback with dynamic flow progression
// ============================================================================

async function handlePostback(event) {
  const senderId = event.sender?.id;
  let payload, title;

  if (event.postback) {
    payload = event.postback.payload;
    title = event.postback.title;
  } else if (event.message?.quick_reply) {
    payload = event.message.quick_reply.payload;
    title = event.message.text;
  }

  if (!senderId || !payload) {
    console.warn("⚠️ Missing senderId or payload");
    return;
  }

  console.log("📲 Postback/QuickReply received:", { senderId, payload, title });

  // ============================================================================
  // HANDLE NESTED QUICK REPLY SELECTIONS
  // ============================================================================
  if (payload.startsWith("QR_NESTED_")) {
    console.log("→ Processing nested quick reply selection");

    const conversation = await ConversationState.findOne({
      igUserId: senderId,
      status: "active",
      expiresAt: { $gt: new Date() },
    }).sort({ startedAt: -1 });

    if (!conversation) {
      console.warn("⚠️ No conversation found");
      return;
    }

    const nestedConfig = conversation.currentNestedQuickReplyConfig;

    if (!nestedConfig) {
      console.warn("⚠️ No nested config found");
      return;
    }

    const { nestedConfig: config, parentNodeId } = nestedConfig;

    // Extract option ID from payload
    const parts = payload.split("_");
    const selectedOptionId = parts[parts.length - 1];

    const selectedNestedOption = config.replyOptions?.find(
      (opt) => String(opt.id) === selectedOptionId
    );

    if (!selectedNestedOption) {
      console.warn("⚠️ Nested option not found:", selectedOptionId);
      return;
    }

    console.log("✅ User selected nested option:", selectedNestedOption.text);

    conversation.addHistory({
      flowId: String(parentNodeId),
      flowName: "NESTED_QUICK_REPLY",
      messageSent: config.quickReplyQuestion,
      userReply: selectedNestedOption.text,
      userPayload: payload,
    });

    const creds = await ensureFreshPageTokenForUser(conversation.userId);
    const accessToken = creds.fbPageAccessToken;
    const fbPageId = creds.fbPageId;

    if (!accessToken || !fbPageId) {
      console.error("❌ Missing credentials");
      conversation.markError(new Error("Missing credentials"));
      await conversation.save();
      return;
    }

    // ✅ Execute nested option actions
    if (selectedNestedOption.actions && selectedNestedOption.actions.length > 0) {
      try {
        const actionResult = await executeAction({
          action: selectedNestedOption.actions[0],
          selectedOption: selectedNestedOption,
          conversation,
          senderId,
          accessToken,
          fbPageId,
          parentNodeId,
        });

        if (actionResult.isNested) {
          // Another nested level - wait for user response
          return;
        }
      } catch (err) {
        console.error("❌ Failed to execute nested action:", err.message);
        conversation.markError(err);
        await conversation.save();
        return;
      }
    }

    // ✅ Clear nested config and find next node
    conversation.currentNestedQuickReplyConfig = null;

    const flowConfig = conversation.flowConfig || [];
    const nextNode = getNextFlowNode(flowConfig, parentNodeId);

    if (nextNode) {
      console.log(`→ Moving to next node: ${nextNode.id} (${nextNode.type})`);

      try {
        await executeFlowNode({
          flowNode: nextNode,
          conversation,
          senderId,
          accessToken,
          fbPageId,
        });

        console.log("✅ Next flow node executed");
        return;
      } catch (err) {
        console.error("❌ Failed to execute next node:", err.message);
        conversation.markError(err);
        await conversation.save();
        return;
      }
    } else {
      console.log("🏁 Flow completed (no more nodes)");
      conversation.markCompleted();
      await conversation.save();
      await Automation.updateOne(
        { _id: conversation.automationId },
        { $inc: { "runStats.flowConversationsCompleted": 1 } }
      );
      return;
    }
  }

  // ============================================================================
  // HANDLE FLOW_START (Initial Button Click)
  // ============================================================================
  if (payload.startsWith("FLOW_START_")) {
    console.log("→ User clicked initial button");

    const automationId = payload.replace("FLOW_START_", "");

    const conversation = await ConversationState.findOne({
      igUserId: senderId,
      automationId: automationId,
      status: "active",
      expiresAt: { $gt: new Date() },
    }).sort({ startedAt: -1 });

    if (!conversation) {
      console.log("ℹ️ No conversation found");
      return;
    }

    const flowConfig = conversation.flowConfig || [];
    const firstNode = flowConfig[0];

    if (!firstNode) {
      console.error("❌ No flow nodes configured");
      return;
    }

    const creds = await ensureFreshPageTokenForUser(conversation.userId);
    const accessToken = creds.fbPageAccessToken;
    const fbPageId = creds.fbPageId;

    if (!accessToken || !fbPageId) {
      console.error("❌ Missing credentials");
      return;
    }

    try {
      await executeFlowNode({
        flowNode: firstNode,
        conversation,
        senderId,
        accessToken,
        fbPageId,
      });

      console.log("✅ First flow node executed");
    } catch (err) {
      console.error("❌ Failed to execute first node:", err.message);
      conversation.markError(err);
      await conversation.save();
    }

    return;
  }

  // ============================================================================
  // HANDLE FOLLOWCHECK RECHECK
  // ============================================================================
  if (payload.startsWith("FOLLOWCHECK_RECHECK_")) {
    const nodeId = payload.replace("FOLLOWCHECK_RECHECK_", "");

    const conversation = await ConversationState.findOne({
      igUserId: senderId,
      status: "active",
      expiresAt: { $gt: new Date() },
    }).sort({ startedAt: -1 });

    if (!conversation) {
      console.log("ℹ️ No conversation found");
      return;
    }

    const flowConfig = conversation.flowConfig || [];
    const followCheckNode = flowConfig.find((node) => String(node.id) === String(nodeId));

    if (!followCheckNode) {
      console.error("❌ FollowCheck node not found");
      return;
    }

    const creds = await ensureFreshPageTokenForUser(conversation.userId);
    const accessToken = creds.fbPageAccessToken;
    const fbPageId = creds.fbPageId;

    if (!accessToken || !fbPageId) {
      console.error("❌ Missing credentials");
      return;
    }

    try {
      const result = await executeFollowCheckNode({
        flowNode: followCheckNode,
        conversation,
        senderId,
        accessToken,
        fbPageId,
      });

      if (result.completed) {
        // User is now following - move to next node
        const nextNode = getNextFlowNode(flowConfig, nodeId);

        if (nextNode) {
          console.log(`→ Moving to next node after followCheck: ${nextNode.id}`);

          await executeFlowNode({
            flowNode: nextNode,
            conversation,
            senderId,
            accessToken,
            fbPageId,
          });
        } else {
          console.log("🏁 Flow completed");
          conversation.markCompleted();
          await conversation.save();
          await Automation.updateOne(
            { _id: conversation.automationId },
            { $inc: { "runStats.flowConversationsCompleted": 1 } }
          );
        }
      }
    } catch (err) {
      console.error("❌ FollowCheck recheck failed:", err.message);
      conversation.markError(err);
      await conversation.save();
    }

    return;
  }

  // ============================================================================
  // HANDLE QUICK REPLY SELECTIONS
  // ============================================================================
  const conversation = await ConversationState.findOne({
    igUserId: senderId,
    status: "active",
    expiresAt: { $gt: new Date() },
  }).sort({ startedAt: -1 });

  if (!conversation) {
    console.log("ℹ️ No active conversation found");
    return;
  }

  const currentFlowId = conversation.currentFlowId;
  const flowConfig = conversation.flowConfig || [];

  const currentNode = flowConfig.find(
    (node) => String(node.id) === String(currentFlowId)
  );

  if (!currentNode) {
    console.error("❌ Current node not found:", currentFlowId);
    return;
  }

  if (currentNode.type === "quickReply") {
    console.log("→ Processing quick reply selection");

    const selectedOption = currentNode.replyOptions?.find(
      (opt) =>
        payload === `QR_${currentNode.id}_${opt.id}` ||
        payload === `QR_${currentNode.id}_${currentNode.replyOptions.indexOf(opt)}`
    );

    if (!selectedOption) {
      console.warn("⚠️ Selected option not found");
      return;
    }

    console.log("✅ User selected:", selectedOption.text);

    conversation.addHistory({
      flowId: String(currentNode.id),
      flowName: "QUICK_REPLY",
      messageSent: currentNode.config.quickReplyQuestion,
      userReply: selectedOption.text,
      userPayload: payload,
    });

    const creds = await ensureFreshPageTokenForUser(conversation.userId);
    const accessToken = creds.fbPageAccessToken;
    const fbPageId = creds.fbPageId;

    if (!accessToken || !fbPageId) {
      console.error("❌ Missing credentials");
      conversation.markError(new Error("Missing credentials"));
      await conversation.save();
      return;
    }

    // ✅ Execute option actions
    if (selectedOption.actions && selectedOption.actions.length > 0) {
      try {
        const actionResult = await executeAction({
          action: selectedOption.actions[0],
          selectedOption,
          conversation,
          senderId,
          accessToken,
          fbPageId,
          parentNodeId: currentNode.id,
        });

        if (actionResult.isNested) {
          // Nested quick reply - wait for user response
          return;
        }
      } catch (err) {
        console.error("❌ Failed to execute action:", err.message);
        conversation.markError(err);
        await conversation.save();
        return;
      }
    }

    // ✅ Find and execute next node
    const nextNode = getNextFlowNode(flowConfig, currentNode.id);

    if (nextNode) {
      console.log(`→ Moving to next node: ${nextNode.id} (${nextNode.type})`);

      try {
        await executeFlowNode({
          flowNode: nextNode,
          conversation,
          senderId,
          accessToken,
          fbPageId,
        });

        console.log("✅ Next flow node executed");
      } catch (err) {
        console.error("❌ Failed to execute next node:", err.message);
        conversation.markError(err);
        await conversation.save();
      }
    } else {
      console.log("🏁 Flow completed (no more nodes)");
      conversation.markCompleted();
      await conversation.save();
      await Automation.updateOne(
        { _id: conversation.automationId },
        { $inc: { "runStats.flowConversationsCompleted": 1 } }
      );
    }
  }
}





// Health check
app.get("/", (_req, res) => res.status(200).send("ok"));
app.get("/health", (_req, res) => res.status(200).send("ok"));

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Pub/Sub processor listening on port ${PORT} at ${new Date().toISOString()}`);
});
