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
import { persistInboxMessage } from "./services/inboxPersistence.js";
import { publishInboxMessageHTTP, publishConversationUpdate } from "./services/realtimePublisher.js";
import levenshtein from "fast-levenshtein";
import agenda from "./services/agenda.js";


const app = express();
app.use(express.json({ type: "*/*" }));

// Config
const PORT = 8080;
const PUBSUB_TOKEN = process.env.PUBSUB_TOKEN || "";
const META_APP_ID = process.env.META_APP_ID;
const META_APP_SECRET = process.env.META_APP_SECRET;
const FB_API = "https://graph.facebook.com/v24.0";
const DAY_MS = 24 * 60 * 60 * 1000;

const db_username = process.env.MONGO_DB_USER;
const db_password = process.env.MONGO_DB_PASS;

var MONGO_URI = 'mongodb+srv://'+db_username+':'+db_password+'@cluster0.itfkrwb.mongodb.net/?appName=Cluster0';

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
  }
}


const COMMENT_DELAY_BUCKETS = [35, 42, 60, 86, 120];

async function computeHumanDelayMs() {
  const bucket =
    COMMENT_DELAY_BUCKETS[
      Math.floor(Math.random() * COMMENT_DELAY_BUCKETS.length)
    ];
  const seconds = Math.floor(Math.random() * bucket) + 1;
  return seconds * 1000;
}

// ---------- Utils ----------
function normalize(str = "") {
  return String(str).toLowerCase().trim();
}

// ---------- SMART COMMENT NORMALIZATION ----------

// Reduce repeated characters: priceeee -> price
function collapseRepeats(str) {
  return str.replace(/(.)\1{2,}/g, "$1");
}

// Join spaced letters: p r i c e -> price
function joinSpacedLetters(str) {
  return str.replace(/\b(?:[a-z]\s){2,}[a-z]\b/g, (match) =>
    match.replace(/\s+/g, "")
  );
}

// Full normalization pipeline for comments
function normalizeComment(text = "") {
  let t = String(text).toLowerCase();

  // Replace symbols with space
  t = t.replace(/[-_.,!?@#$%^&*()+=/\\[\]{}|:;"'<>\n\r]/g, " ");

  // Collapse extra spaces
  t = t.replace(/\s+/g, " ").trim();

  // Join spaced letters
  t = joinSpacedLetters(t);

  // Reduce repeated characters
  t = collapseRepeats(t);

  return t;
}

function keywordMatch(normalizedText, keywords = []) {
  if (!normalizedText || !keywords.length) return false;

  const tokens = normalizedText.split(" ");

  return keywords.some((kw) => {
    const k = normalizeComment(kw);

    // 1️⃣ Exact match
    if (tokens.includes(k)) return true;

    // 2️⃣ Substring match
    if (normalizedText.includes(k)) return true;

    // 3️⃣ Levenshtein fuzzy match
    return tokens.some((token) => {
      return levenshtein.get(token, k) <= 1;
    });
  });
}




function escapeRegex(string) {
  return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
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

async function daysLeft(expiry) {
  if (!expiry) return -Infinity;
  return Math.floor((new Date(expiry).getTime() - Date.now()) / DAY_MS);
}

async function refreshFbTokensForUser(user) {
  // 1️⃣ Exchange for a fresh Long-Lived User Token
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

  // Calculate expiry (Prefer API 'expires_in', fallback to 58 days)
  const expiresInSec = llResp.data?.expires_in;
  let newUserExpiry;
  if (expiresInSec && Number(expiresInSec) > 0) {
    newUserExpiry = new Date(Date.now() + Number(expiresInSec) * 1000);
  } else {
    newUserExpiry = new Date(Date.now() + 58 * DAY_MS);
  }

  // 2️⃣ Refresh Page Token (Using /me/accounts Workaround)
  let newPageToken = user.fbPageAccessToken || null;

  if (user.fbPageId) {
    try {
      // Query /me/accounts instead of /<page_id> to avoid pages_read_engagement requirement
      const accountsResp = await axios.get(`${FB_API}/me/accounts`, {
        params: {
          access_token: newUserLL,
          fields: "id,access_token",
          limit: 100, // Fetch enough pages to ensure we find ours
        },
      });

      const pages = accountsResp.data?.data || [];
      
      // Find the page matching the user's stored fbPageId
      const targetPage = pages.find((p) => p.id === user.fbPageId);

      if (targetPage && targetPage.access_token) {
        newPageToken = targetPage.access_token;
        console.log(`✅ Page Token refreshed via /me/accounts for Page ID: ${user.fbPageId}`);
      } else {
        console.warn(`⚠️ Page ID ${user.fbPageId} not found in user's account list during refresh.`);
      }
    } catch (pageErr) {
      console.error("❌ Failed to fetch /me/accounts during refresh:", pageErr?.response?.data || pageErr.message);
      // We do not throw here; we still want to save the fresh User Token even if Page Token fetch fails
    }
  }

  // 3️⃣ Save updates to DB
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

  const remain = await daysLeft(user.fbLongLivedTokenExpiry);

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
    return { proceed: true, lockId: newLock._id };

    
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
        .map((b) => {
          // 1. Check if it is explicitly a Postback button (has payload)
          if (b.type === "postback" || b.payload) {
            return {
              type: "postback",
              title: (b.text || b.title || "Select").toString().slice(0, 20),
              payload: (b.payload || "EMPTY_PAYLOAD").toString(),
            };
          }

          // 2. Otherwise, treat it as a URL button (Standard behavior)
          return {
            type: "web_url",
            url: (b.url || b.link || "").toString(),
            title: (b.text || b.title || "Open").toString().slice(0, 20),
          };
        });

      const body = {
        recipient: sendRecipient,
        message: {
          attachment: {
            type: "template",
            payload: {
              template_type: "button",
              text: message || "Please choose:",
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

// ========== NEW: startDirectFlow (For Auto DM triggers) ==========


async function startDirectFlow({
  automation,
  igUserId,
  pageAccessToken,
  fbPageId,
  messageId
}) {
  try {
    console.log(`🚀 Starting Direct Flow for automation: ${automation._id}`);

    // VALIDATION: We need the button text 
    if (!automation.buttonText) {
       console.warn("⚠️ No buttonText found.");
    }

    // 1. Create ConversationState 
    // We set currentFlowId to a special flag: "awaiting_initial_button_click"
    const conversation = await ConversationState.create({
      userId: automation.userId,
      automationId: automation._id,
      commentId: null, 
      messageId,
      igUserId: igUserId,
      currentFlowId: "awaiting_initial_button_click", // <--- IMPORTANT STATE
      flowConfig: automation.flowNodes,
      conversationHistory: [
        {
          flowId: "initial_dm",
          flowName: "DIRECT_TRIGGER_INITIAL_MSG",
          messageSent: automation.dmMessage,
          userReply: null,
          timestamp: new Date(),
        },
      ],
      status: "active",
      startedAt: new Date(),
      expiresAt: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000), 
    });

    console.log("✅ ConversationState created. Status: awaiting_initial_button_click");

    // 2. Send the Initial DM with BUTTON TEMPLATE (Not Quick Reply)
    const buttonPayload = {
      recipient: { id: igUserId },
      message: {
        attachment: {
          type: "template",
          payload: {
            template_type: "button",
            text: automation.dmMessage, // "Hey! Thanks..."
            buttons: [
              {
                type: "postback", 
                title: automation.buttonText || "Continue", // "Send Link"
                payload: "INITIAL_DM_CLICKED" 
              }
            ]
          }
        }
      }
    };

    await axios.post(
      `https://graph.facebook.com/v21.0/${fbPageId}/messages`,
      buttonPayload,
      { params: { access_token: pageAccessToken } }
    );

    console.log(`✅ Sent Initial DM with button: "${automation.buttonText}"`);
    
    return { ok: true };

  } catch (err) {
    console.error("❌ Error in startDirectFlow:", err.message);
    throw err;
  }
}



// async function handleTextMessage(event, businessId) {
//   const senderId = event.sender?.id;
//   const messageId = event.message?.mid; // Message ID from Meta

//   // --- NEW: IGNORE ECHOES ---
//   if (event.message?.is_echo) {
//     return;
//   }

//   // 🔥 FIX 1: Use Meta timestamp, NOT server time
//   const createdAtPlatform = event.timestamp
//     ? new Date(event.timestamp)
//     : new Date();

//   /* =========================================================
//      🔥 FIX 2: NORMALIZE MESSAGE (TEXT / REEL / ATTACHMENT)
//      ========================================================= */

//   let type = "text";
//   let text = event.message?.text || null;
//   let mediaUrl = null;
//   let mediaType = null;
//   let action = null;

//   const attachment = event.message?.attachments?.[0];

//   // Image
//   if (attachment?.type === "image") {
//     type = "image";
//     mediaType = "image";
//     mediaUrl = attachment.payload?.url || null;
//   }
//   // Video
//   else if (attachment?.type === "video") {
//     type = "video";
//     mediaType = "video";
//     mediaUrl = attachment.payload?.url || null;
//   }
//   // Unsupported (reel / post / story share)
//   else if (event.message?.is_unsupported) {
//     type = "system";
//     text = "Shared a reel";
//     action = {
//       label: "View on Instagram",
//       url: "https://www.instagram.com/direct/inbox/",
//     };
//   }
//   // Any other attachment without text
//   else if (!text) {
//     type = "system";
//     text = "Shared an attachment";
//   }

//   console.log("💬 Incoming message:", {
//     senderId,
//     type,
//     text,
//     mediaUrl,
//     createdAtPlatform,
//   });

//   /* =========================================================
//      1️⃣ Resolve creator from business IG ID
//      ========================================================= */

//   const creator = await User.findOne({ igUserId: businessId })
//     .select("_id")
//     .lean();

//   if (creator) {
//     try {
//       const { conversation, message } = await persistInboxMessage({
//         creatorId: creator._id,
//         businessIgUserId: businessId,
//         senderIgUserId: senderId,
//         igMessageId: messageId,

//         // 🔥 pass normalized fields
//         type,
//         text,
//         mediaUrl,
//         mediaType,
//         action,

//         createdAt: createdAtPlatform,
//       });

//       /* =========================================================
//          2️⃣ Realtime publish (SAFE FOR ALL MESSAGE TYPES)
//          ========================================================= */

// await publishInboxMessageHTTP({
//   creatorId: creator._id.toString(),
//   conversationId: conversation._id.toString(),

//   // 1️⃣ Message payload (unchanged)
//   message: {
//     _id: message._id.toString(),

//     sender: message.sender,
//     senderType: message.senderType,
//     senderTypeRef: message.senderTypeRef,

//     type: message.type,
//     text: message.text,
//     mediaUrl: message.mediaUrl,
//     mediaType: message.mediaType,
//     action: message.action,

//     createdAtPlatform: message.createdAtPlatform,
//     isRead: message.isRead,
//   },

//   // 2️⃣ 🔥 AUTHORITATIVE CONVERSATION SNAPSHOT
//   conversation: {
//     unreadCount: conversation.unreadCount,
//     lastMessage: conversation.lastMessage,
//     lastActivityAt: conversation.lastActivityAt,
//     lastParticipantMessageAt: conversation.lastParticipantMessageAt
//   },
// });


//   await publishConversationUpdate({
//         creatorId: creator._id.toString(),
//         conversationId: conversation._id.toString(),
//         update: {
//           lastMessage: conversation.lastMessage,
//           lastActivityAt: conversation.lastActivityAt,
//           unreadCount: conversation.unreadCount,
//     lastParticipantMessageAt: conversation.lastParticipantMessageAt

//         }
//       });

//     } catch (e) {
//       console.error("❌ Inbox persistence failed:", e.message);
//     }
//   }

//   /* =========================================================
//      Existing logic BELOW — UNCHANGED
//      ========================================================= */

//   const normalizedText = normalize(text || "");

//   // ========================================================================
//   // PATH A: EXISTING FLOW (User is responding to a Comment->Private Reply)
//   // ========================================================================
//   const conversation = await ConversationState.findOne({
//     igUserId: senderId,
//     status: "active",
//     expiresAt: { $gt: new Date() },
//   }).sort({ startedAt: -1 });

//   if (conversation) {
//     if (conversation.currentFlowId === "awaiting_user_response") {
//       console.log("✅ User responded to initial message, 24hr window now open");

//       const creds = await ensureFreshPageTokenForUser(conversation.userId);
//       const accessToken = creds.fbPageAccessToken;
//       const fbPageId = creds.fbPageId;

//       const initialNode =
//         conversation.flowConfig.initial || conversation.flowConfig[0];

//       try {
//         await sendFlowMessage({
//           recipient: { id: String(senderId) },
//           flowNode: initialNode,
//           pageAccessToken: accessToken,
//           fbPageId: fbPageId,
//         });

//         conversation.currentFlowId = String(initialNode.id || "initial");
//         conversation.addHistory({
//           flowId: "initial_response",
//           flowName: "USER_RESPONDED_TO_DM",
//           messageSent: initialNode.message,
//           userReply: text,
//         });
//         await conversation.save();

//         console.log("✅ Quick replies sent after user text response");
//       } catch (err) {
//         console.error("❌ Failed to send quick replies:", err.message);
//         conversation.markError(err);
//         await conversation.save();
//       }
//     }
//   }

//   // ========================================================================
//   // PATH B: NEW TRIGGER (User sends a DM Keyword like "Coach", "Link")
//   // ========================================================================
//   else {
//     console.log(
//       `🔍 No active conversation. Checking keywords for Business ID: ${businessId}`
//     );

//     if (!businessId) {
//       console.warn("⚠️ Cannot process DM trigger: Missing businessId");
//       return;
//     }

//     let user = await User.findOne({ igUserId: businessId }).lean();
//     const keywordRegex = new RegExp(
//       `^${escapeRegex(normalizedText)}$`,
//       "i"
//     );

//     const automation = await Automation.findOne({
//       userId: user._id,
//       postType: "autodm",
//       platform: "instagram",
//       status: "active",
//       keywords: { $in: [keywordRegex] },
//     }).lean();

//     if (!automation) {
//       console.log(`ℹ️ No automation found for keyword: "${normalizedText}"`);
//       return;
//     }

//     console.log(`🎯 Keyword Match! Starting Automation: ${automation._id}`);

//     try {
//       await ActionLock.create({
//         automationId: automation._id,
//         postType: "autodm",
//         postId: "automDM12345",
//         igUserId: senderId,
//         commentId: messageId,
//         channel: "private",
//         state: "sent",
//         reservedAt: new Date(),
//         sentAt: new Date(),
//       });
//     } catch (err) {
//       if (err.code === 11000) {
//         console.log("⚠️ Duplicate DM webhook event detected. Skipping.");
//         return;
//       }
//       console.error("ActionLock error", err);
//     }

//     const creds = await ensureFreshPageTokenForUser(user._id);

//     if (!creds.fbPageAccessToken) {
//       console.error("❌ Could not get access token for user");
//       return;
//     }

//     await startDirectFlow({
//       automation,
//       igUserId: senderId,
//       pageAccessToken: creds.fbPageAccessToken,
//       fbPageId: creds.fbPageId || businessId,
//       messageId,
//     });
//   }
// }


// ========== PUB/SUB ENDPOINT: COMMENTS ==========

async function handleTextMessage(event, businessId) {
  const senderId = event.sender?.id;
  const messageId = event.message?.mid;

  // =========================================================
  // 0️⃣ Ignore invalid / echo messages
  // =========================================================
  if (!senderId || !messageId) return;
  if (event.message?.is_echo) return;

  const createdAtPlatform = event.timestamp
    ? new Date(event.timestamp)
    : new Date();

  // =========================================================
  // 1️⃣ Normalize incoming message (TEXT / MEDIA / SYSTEM)
  // =========================================================
  let type = "text";
  let text = event.message?.text || null;
  let mediaUrl = null;
  let mediaType = null;
  let action = null;

  const attachment = event.message?.attachments?.[0];

  if (attachment?.type === "image") {
    type = "image";
    mediaType = "image";
    mediaUrl = attachment.payload?.url || null;
  } else if (attachment?.type === "video") {
    type = "video";
    mediaType = "video";
    mediaUrl = attachment.payload?.url || null;
  } else if (event.message?.is_unsupported) {
    type = "system";
    text = "Shared unsupported content";
  } else if (!text) {
    type = "system";
    text = "Shared an attachment";
  }

  const normalizedText = normalize(text || "");

  // =========================================================
  // 2️⃣ GATE 1: Resolve creator (businessId → user)
  // =========================================================
  const creator = await User.findOne({ igUserId: businessId })
    .select("_id")
    .lean();

  if (!creator) {
    console.log("ℹ️ No creator found for businessId:", businessId);
    return;
  }

  // =========================================================
  // 3️⃣ Inbox persistence (NON-BLOCKING, ALWAYS SAFE)
  // =========================================================
  try {
    const { conversation, message } = await persistInboxMessage({
      creatorId: creator._id,
      businessIgUserId: businessId,
      senderIgUserId: senderId,
      igMessageId: messageId,
      type,
      text,
      mediaUrl,
      mediaType,
      action,
      createdAt: createdAtPlatform,
    });

    await publishInboxMessageHTTP({
      creatorId: creator._id.toString(),
      conversationId: conversation._id.toString(),
      message: {
        _id: message._id.toString(),
        sender: message.sender,
        senderType: message.senderType,
        senderTypeRef: message.senderTypeRef,
        type: message.type,
        text: message.text,
        mediaUrl: message.mediaUrl,
        mediaType: message.mediaType,
        action: message.action,
        createdAtPlatform: message.createdAtPlatform,
        isRead: message.isRead,
      },
      conversation: {
        unreadCount: conversation.unreadCount,
        lastMessage: conversation.lastMessage,
        lastActivityAt: conversation.lastActivityAt,
        lastParticipantMessageAt:
          conversation.lastParticipantMessageAt,
      },
    });

    await publishConversationUpdate({
      creatorId: creator._id.toString(),
      conversationId: conversation._id.toString(),
      update: {
        lastMessage: conversation.lastMessage,
        lastActivityAt: conversation.lastActivityAt,
        unreadCount: conversation.unreadCount,
        lastParticipantMessageAt:
          conversation.lastParticipantMessageAt,
      },
    });
  } catch (err) {
    console.error("❌ Inbox persistence failed:", err.message);
  }

  // =========================================================
  // 4️⃣ PATH A: Existing active conversation → continue flow
  // =========================================================
  const activeConversation = await ConversationState.findOne({
    igUserId: senderId,
    status: "active",
    expiresAt: { $gt: new Date() },
  }).sort({ startedAt: -1 });

  if (activeConversation) {
    // Flow continuation handled elsewhere
    return;
  }

  // =========================================================
  // 5️⃣ GATE 2: Is ANY autodm automation active for this page?
  // =========================================================
  const hasAutoDM = await Automation.exists({
    igUserId: businessId,
    postType: "autodm",
    platform: "instagram",
    status: "active",
  });

  if (!hasAutoDM) {
    console.log("ℹ️ No autodm automations for business:", businessId);
    return;
  }

  // =========================================================
  // 6️⃣ GATE 3: Keyword → Automation match
  // =========================================================
  const keywordRegex = new RegExp(`^${escapeRegex(normalizedText)}$`, "i");

  const automation = await Automation.findOne({
    igUserId: businessId,
    postType: "autodm",
    platform: "instagram",
    status: "active",
    keywords: { $in: [keywordRegex] },
  }).lean();

  if (!automation) {
    console.log("ℹ️ No keyword match for text:", normalizedText);
    return;
  }

  // =========================================================
  // 7️⃣ GATE 4 (CRITICAL): Reserve ActionLock FIRST
  // =========================================================
  const { proceed } = await reserveAction({
    automationId: automation._id,
    postId: "autodm",
    igUserId: senderId,
    commentText: text || "",
    commentId: messageId,
    channel: "private",
  });

  if (!proceed) {
    console.log("⚠️ Duplicate DM trigger blocked by ActionLock");
    return;
  }

  // =========================================================
  // 8️⃣ Execute automation (SAFE TO RUN ONCE)
  // =========================================================
  const creds = await ensureFreshPageTokenForUser(automation.userId);

  if (!creds.fbPageAccessToken) {
    console.error("❌ Missing page access token");
    return;
  }

  await startDirectFlow({
    automation,
    igUserId: senderId,
    pageAccessToken: creds.fbPageAccessToken,
    fbPageId: creds.fbPageId || businessId,
    messageId,
  });

  console.log("🚀 AutoDM flow started safely:", automation._id);
}



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

       if (!c.fromUserId) {
    console.warn(`⚠️ Skipping comment event without fromUserId:`, {
      commentId: c.commentId,
      text: c.text?.substring(0, 50),
      mediaId: c.mediaId,
      eventType: 'comment'
    });
    continue;
  }
      // ============================================================================
      // STEP 1: Find existing post-specific automation
      // ============================================================================
      let automation = await Automation.findOne({
        platform: "instagram",
        status: "active",
        postId: c.mediaId,
        igUserId: c.pageId
      }).lean();

      console.log('CCCCCCCCCCCCCCC : ', c);

      // ============================================================================
      // STEP 2: If not found, check for future post template and clone it
      // ============================================================================
      if (!automation) {
        console.log(`🔍 No post-specific automation found for media: ${c.mediaId}`);
        console.log(`🔍 Checking for future post template...`);

        // Find active future post template
        const template = await Automation.findOne({
          platform: "instagram",
          status: "active",
          postType: "futurepost",
          igUserId: c.pageId
        }).lean();

        if (template) {
          console.log(`📋 Future post template found: ${template._id}`);
          
          // ============================================================================
          // ATOMIC CLONE: Use findOneAndUpdate with upsert to prevent duplicates
          // ============================================================================
          try {
            automation = await Automation.findOneAndUpdate(
              {
                // Match criteria - prevents duplicates
                postId: c.mediaId,
                platform: "instagram",
              },
              {
                $setOnInsert: {
                  // Copy all fields from template
                  userId: template.userId,
                  platform: template.platform,
                  postType: "post", // ✅ Changed from 'futurepost'
                  postId: c.mediaId, // ✅ Set actual postId
                  repliedCount: 0,
                  thumbnail: template.thumbnail || null,
                  postLive: true,
                  igUserId: template.igUserId,
                  lastCheckedAt: new Date(),
                  caption: template.caption || null,
                  dmMessage: template.dmMessage,
                  buttonText: template.buttonText,
                  flowNodes: template.flowNodes || [],
                  keywords: template.keywords || [],
                  hasReply: template.hasReply || false,
                  replyComments: template.replyComments || [],
                  status: "active",
                  createdAt: new Date(),
                  clonedFrom: template._id, // ✅ Track template origin
                }
              },
              {
                upsert: true, // Create if doesn't exist
                new: true, // Return the new document
                setDefaultsOnInsert: true,
              }
            );

            console.log(`✅ Cloned future post template to new automation:`, {
              newAutomationId: automation._id,
              postId: c.mediaId,
              templateId: template._id,
            });

          } catch (cloneErr) {
            console.error("❌ Failed to clone template:", cloneErr.message);
            // If cloning fails, try to fetch if it was created by another request
            automation = await Automation.findOne({
              platform: "instagram",
              status: "active",
              postId: c.mediaId,
              igUserId: c.pageId
            }).lean();
          }
        } else {
          console.log(`ℹ️ No future post template found`);
        }
      }

      // ============================================================================
      // STEP 3: If still no automation, skip this comment
      // ============================================================================
      if (!automation) {
        console.log("ℹ️ No automation found (neither specific nor template) for media:", c.mediaId);
        continue;
      }

      // ============================================================================
      // STEP 4: Keyword matching
      // ============================================================================
      console.log(`✅ Processing comment with automation: ${automation._id}`);

      const normalizedComment = normalizeComment(c.text);

      // Normalize automation keywords
      const normalizedKeywords = (automation.keywords || [])
        .map(normalizeComment)
        .filter(Boolean);

      // Smart keyword match
      const matched =
        normalizedKeywords.length === 0 ||
        keywordMatch(normalizedComment, normalizedKeywords);

      if (!matched) {
        console.log("ℹ️ No keyword match for comment:", {
          raw: c.text,
          normalized: normalizedComment,
        });
        continue;
      }

      console.log("🎯 Comment matched automation:", {
        raw: c.text,
        normalized: normalizedComment,
      });

      // ============================================================================
      // STEP 5: Fetch tokens (with caching)
      // ============================================================================
      let creds = userTokenCache.get(String(automation.userId));
      if (!creds) {
        creds = await ensureFreshPageTokenForUser(automation.userId);
        userTokenCache.set(String(automation.userId), creds);
      }

      const { fbPageAccessToken: accessToken, fbPageId } = creds;
      if (!accessToken || !fbPageId) {
        console.warn("⚠️ Missing tokens for user:", automation.userId);
        continue;
      }

      // ============================================================================
      // STEP 6: Gather available replies
      // ============================================================================
      let replyCandidates = [];
      
      if (Array.isArray(automation.replyComments) && automation.replyComments.length > 0) {
        replyCandidates = automation.replyComments;
      } else if (automation.replyComment) {
        replyCandidates = [automation.replyComment];
      }

      // ============================================================================
      // STEP 7: Compute human-like delay
      // ============================================================================
      const delayMs = await computeHumanDelayMs();
      const scheduledAt = Date.now() + delayMs;

      // ============================================================================
      // STEP 8: PUBLIC REPLY (QUEUE ONLY)
      // ============================================================================
      if (automation.hasReply && replyCandidates.length > 0) {
        const replyTextToSend =
          replyCandidates[Math.floor(Math.random() * replyCandidates.length)];

        const { proceed, lockId } = await reserveAction({
          automationId: automation._id,
          postId: c.mediaId,
          igUserId: c.fromUserId,
          commentText: c.text,
          commentId: c.commentId,
          channel: "public",
        });

        if (!proceed) {
          console.log("ℹ️ Public reply already reserved:", c.commentId);
        } else {
          const res = await ActionLock.updateOne(
            { _id: lockId, state: "reserved" },
            {
              $set: {
                state: "queued",
                scheduledAt: new Date(scheduledAt),
                payload: {
                  replyText: replyTextToSend,
                  pageId: fbPageId,
                  creatorId: automation.userId,
                  automationId: automation._id
                },
              },
            }
          );

          if (res.modifiedCount !== 1) {
            console.log("ℹ️ ActionLock not queued (already processed)");
          } else {
            // Schedule Agenda job (PUBLIC)
            await agenda.schedule(
              new Date(scheduledAt),
              "process_action_lock",
              { actionLockId: lockId }
            );

            console.log("🕒 Public reply queued:", {
              commentId: c.commentId,
              scheduledAt: new Date(scheduledAt).toISOString(),
            });
          }
        }
      }

      // ============================================================================
      // STEP 9: PRIVATE DM (QUEUE ONLY)
      // ============================================================================
      if (automation.dmMessage && automation.buttonText) {
        const { proceed, lockId } = await reserveAction({
          automationId: automation._id,
          postId: c.mediaId,
          igUserId: c.fromUserId,
          commentText: c.text,
          commentId: c.commentId,
          channel: "private",
        });

        if (!proceed) {
          console.log("ℹ️ Private DM already reserved:", c.commentId);
        } else {
          const res = await ActionLock.updateOne(
            { _id: lockId, state: "reserved" },
            {
              $set: {
                state: "queued",
                scheduledAt: new Date(scheduledAt),
                payload: {
                  dmMessage: automation.dmMessage,
                  buttonText: automation.buttonText,
                  automationId: automation._id,
                  pageId: fbPageId,
                  creatorId: automation.userId,
                  igUserId: c.fromUserId,
                  commentId: c.commentId,
                },
              },
            }
          );

          if (res.modifiedCount !== 1) {
            console.log("ℹ️ ActionLock not queued (already processed)");
          } else {
            await agenda.schedule(
              new Date(scheduledAt),
              "process_action_lock",
              { actionLockId: lockId }
            );

            console.log("🕒 Private DM queued:", {
              commentId: c.commentId,
              scheduledAt: new Date(scheduledAt).toISOString(),
            });
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


// ========== UPDATED: PUB/SUB ENDPOINT: MESSAGING ==========
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
      // ✅ EXTRACT BUSINESS ID (Page ID)
      // This is crucial for DM triggers to know WHICH business received the message
      const businessId = entry.id;

      const messaging = entry?.messaging || [];
      console.log(`📬 Processing ${messaging.length} messaging events for Business: ${businessId}`);

      for (const event of messaging) {
        // Handle postback events
        if (event.postback) {
          console.log('Hit Here-1');

          await handlePostback(event);
          continue;
        }

        // Handle quick_reply events (same as postback)
        if (event.message?.quick_reply) {
          console.log('Hit Here-2');

          await handlePostback(event);
          continue;
        }

        // Handle regular text messages
        // ✅ PASS businessId to handleTextMessage
        if (event.message && !event.message.quick_reply) {
          console.log('Hit Here-3');
          console.log('businessId :', businessId);
          await handleTextMessage(event, businessId);
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
// async function executeQuickReplyNode({
//   flowNode,
//   conversation,
//   senderId,
//   pageAccessToken,
//   fbPageId,
// }) {

//   // LIMITATION: Button Templates only support up to 3 buttons.
//   // We slice(0, 3) to prevent API errors.
//   const buttons = (flowNode.replyOptions || [])
//     .slice(0, 3) 
//     .map((option) => ({
//       type: "postback", // Changed to postback for buttons
//       title: (option.text || "Option").toString().slice(0, 20),
//       payload: `QR_${flowNode.id}_${option.id}`,
//     }));

//   if (buttons.length === 0) {
//     throw new Error("No quick reply options available");
//   }

//   const url = `${FB_API}/${fbPageId}/messages`;
  
//   // MODIFIED: Constructing a Button Template Payload instead of Text+QuickReplies
//   const buttonBody = {
//     recipient: { id: String(senderId) },
//     message: {
//       attachment: {
//         type: "template",
//         payload: {
//           template_type: "button",
//           text: flowNode.config?.quickReplyQuestion || "Choose one:",
//           buttons: buttons,
//         },
//       },
//     },
//   };

//   console.log("→ Sending quick_reply as BUTTONS to user:", senderId);

//   try {
//     const { data, status } = await http.post(url, buttonBody, {
//       params: { access_token: pageAccessToken },
//     });
    
//     if (status >= 400) {
//       console.error("Facebook API error data:", data);
//       throw new Error(`Quick replies (buttons) failed: ${JSON.stringify(data)}`);
//     }
    
//     console.log("✅ Quick replies (buttons) sent");

//     // Update conversation
//     conversation.addHistory({
//       flowId: String(flowNode.id),
//       flowName: "QUICK_REPLY_BUTTONS",
//       messageSent: flowNode.config?.quickReplyQuestion,
//       timestamp: new Date(),
//     });

//     conversation.currentFlowId = String(flowNode.id);
//     await conversation.save();

//     return { success: true, data };
    
//   } catch (err) {
//     if (err.response) {
//       console.error("FB API response error status:", err.response.status);
//       console.error("FB API response error data:", err.response.data);
//     } else {
//       console.error("Error in HTTP request:", err.message);
//     }
//     throw err;
//   }
// }

/**
 * Execute a quickReply node with optional image support
 * Uses Generic Template if image exists, Button Template otherwise
 */
async function executeQuickReplyNode({
  flowNode,
  conversation,
  senderId,
  pageAccessToken,
  fbPageId,
}) {

  // Extract image URL from config (uploaded by frontend)
  const imageUrl = flowNode.config?.quickReplyImage || null;
  const questionText = flowNode.config?.quickReplyQuestion || "Choose one:";

  // LIMITATION: Button/Generic Templates only support up to 3 buttons
  const buttons = (flowNode.replyOptions || [])
    .slice(0, 3) 
    .map((option) => ({
      type: "postback",
      title: (option.text || "Option").toString().slice(0, 20),
      payload: `QR_${flowNode.id}_${option.id}`,
    }));

  if (buttons.length === 0) {
    throw new Error("No quick reply options available");
  }

  const url = `${FB_API}/${fbPageId}/messages`;
  
  let messageBody;

  // 🔥 CASE 1: Image exists → Use Generic Template (supports images + buttons)
  if (imageUrl) {
    console.log("→ Sending quick_reply as GENERIC TEMPLATE (with image) to user:", senderId);
    
    messageBody = {
      recipient: { id: String(senderId) },
      message: {
        attachment: {
          type: "template",
          payload: {
            template_type: "generic",
            elements: [
              {
                title: questionText,
                image_url: imageUrl, // 🔥 Image from GCS
                buttons: buttons,
              }
            ]
          }
        }
      }
    };
  } 
  // 🔥 CASE 2: No image → Use Button Template (simpler, no image)
  else {
    console.log("→ Sending quick_reply as BUTTON TEMPLATE (no image) to user:", senderId);
    
    messageBody = {
      recipient: { id: String(senderId) },
      message: {
        attachment: {
          type: "template",
          payload: {
            template_type: "button",
            text: questionText,
            buttons: buttons,
          }
        }
      }
    };
  }

  try {
    const { data, status } = await http.post(url, messageBody, {
      params: { access_token: pageAccessToken },
    });
    
    if (status >= 400) {
      console.error("Facebook API error data:", data);
      throw new Error(`Quick replies failed: ${JSON.stringify(data)}`);
    }
    
    console.log("✅ Quick replies sent successfully");

    // Update conversation
    conversation.addHistory({
      flowId: String(flowNode.id),
      flowName: imageUrl ? "QUICK_REPLY_WITH_IMAGE" : "QUICK_REPLY_BUTTONS",
      messageSent: questionText,
      timestamp: new Date(),
    });

    conversation.currentFlowId = String(flowNode.id);
    await conversation.save();

    return { success: true, data };
    
  } catch (err) {
    if (err.response) {
      console.error("FB API response error status:", err.response.status);
      console.error("FB API response error data:", err.response.data);
    } else {
      console.error("Error in HTTP request:", err.message);
    }
    throw err;
  }
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
    // User is following
    console.log("✅ User is following. Sending Success Message + Button.");
    const igUserIdToUpdate = userDetails.id || senderId;

    if (igUserIdToUpdate) {
        try {
            const updateFields = {
                followsBusiness: userDetails.is_user_follow_business,
                businessFollowsUser: userDetails.is_business_follow_user,
                username: userDetails.username,
                profilePic: userDetails.profile_pic,
            };
            
            // Use updateMany to ensure ALL records associated with this igUserId are updated
            const updateResult = await RepliedComment.updateMany(
                { igUserId: igUserIdToUpdate }, 
                { $set: updateFields }
            );

            console.log(`✅ Database Update: Updated ${updateResult.nModified} RepliedComment records for igUserId ${igUserIdToUpdate}.`);
        } catch (dbErr) {
            console.error("❌ Failed to update RepliedComment records:", dbErr.message);
        }
    }


    const followingButtons = flowNode.followingButtons || [];

    // 1. If buttons exist, we send a BUTTON TEMPLATE (Button Message)
    if (followingButtons.length > 0) {
      
      // Create buttons for the payload
      const buttonPayloads = followingButtons.map(btn => ({
        type: "postback",
        title: btn.text,
        // NEW PAYLOAD FORMAT: To identify this specific button click later
        payload: `FLOW_BTN_${flowNode.id}_${btn.id}` 
      }));

      await sendFlowMessage({
        recipient: { id: senderId },
        flowNode: {
          type: "button", // Force type to button
          message: flowNode.config.followCheckYesMessage,
          buttons: buttonPayloads // Pass the constructed buttons
        },
        pageAccessToken,
        fbPageId,
      });

    } else {
      // 2. Fallback: If no buttons defined, just send text
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

    // REMOVED: The code that auto-executed "executeAction" here. 
    // We now wait for the user to click the button we just sent.

    return { success: true, completed: true };

  } else {
    // User not following - send verification button (Existing logic)
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

    case "downloadFile": {
      
      // Use the redirectUrl from either action type's config
      const redirectUrl = action.config?.redirectUrl || action.config?.downloadFile?.url || "https://example.com";
      const messageText = action.config?.message || "Click below to download:";
      const buttonLabel = action.config?.buttonText || "Download Now";
      
      // If no valid URL is found, log a warning and exit
      if (!redirectUrl || !redirectUrl.startsWith('http')) {
          console.warn(`⚠️ No valid redirect URL found for action type: ${action.type}`);
          return { success: false, error: `Missing URL for ${action.type}` };
      }
      
      console.log(`→ Sending ${action.type} link:`, redirectUrl);

      await sendFlowMessage({
        recipient: { id: senderId },
        flowNode: {
          type: "button",
          message: messageText, 
          buttons: [
            {
              type: "web_url",
              title: buttonLabel,
              url: redirectUrl,
            },
          ],
        },
        pageAccessToken,
        fbPageId,
      });

      console.log(`✅ ${action.type} link sent`);
      return { success: true, completed: true };
    }

  case "quickReply": {
      // ✅ NESTED QUICK REPLY (MODIFIED TO BUTTONS)
      console.log("→ Executing nested quickReply as BUTTONS");

      const nestedConfig = action.config;

      if (!nestedConfig || !nestedConfig.quickReplyQuestion) {
        console.warn("⚠️ Nested quick reply config missing");
        return { success: false, error: "Missing nested config" };
      }

      const nestedOptions = action.replyOptions || nestedConfig.replyOptions || [];
      
      console.log("📋 Nested options found:", nestedOptions.length);
      
      if (nestedOptions.length === 0) {
        console.warn("⚠️ No nested quick reply options found");
        return { success: false, error: "No options" };
      }

      // LIMITATION: Slice to 3 buttons max for Button Template
      const buttonPayloads = nestedOptions.slice(0, 3).map((option) => ({
        type: "postback",
        title: (option.text || "Option").toString().slice(0, 20),
        payload: `QR_NESTED_${parentNodeId}_${option.id}`,
      }));

      console.log("📤 Sending nested buttons to user");

      // Manually sending Button Template here (bypassing sendFlowMessage to ensure structure)
      const url = `${FB_API}/${fbPageId}/messages`;
      const buttonBody = {
        recipient: { id: String(senderId) },
        message: {
          attachment: {
            type: "template",
            payload: {
              template_type: "button",
              text: nestedConfig.quickReplyQuestion || "Choose one:",
              buttons: buttonPayloads,
            },
          },
        },
      };

      try {
        await http.post(url, buttonBody, { params: { access_token: pageAccessToken } });
      } catch (e) {
         console.error("Error sending nested buttons:", e.message);
         throw e;
      }

      console.log("✅ Nested buttons sent");

      // Store nested config for next interaction
      const configToStore = {
        parentNodeId: String(parentNodeId),
        parentOptionId: String(selectedOption.id),
        nestedConfig: {
          ...nestedConfig,
          replyOptions: nestedOptions 
        },
      };

      conversation.currentNestedQuickReplyConfig = configToStore;
      
      conversation.addHistory({
        flowId: String(parentNodeId),
        flowName: "NESTED_BUTTONS_SENT",
        messageSent: nestedConfig.quickReplyQuestion,
        timestamp: new Date(),
      });

      await conversation.save();
      
      console.log("💾 Nested config saved:", {
        parentNodeId: configToStore.parentNodeId,
        optionsCount: nestedOptions.length
      });

      return { success: true, isNested: true };
    }

       case "finishingMessage": {
      const finalMessage = action.config?.finishingMessage || "Thank you! 😊";
      
      console.log("→ Sending finishing message:", finalMessage);

      await sendFlowMessage({
        recipient: { id: senderId },
        flowNode: {
          type: "text",
          message: finalMessage,
        },
        pageAccessToken,
        fbPageId,
      });

      console.log("✅ Finishing message sent");
      
      // Mark conversation as completed since this is the final message
      conversation.addHistory({
        flowId: String(parentNodeId || "final"),
        flowName: "FINISHING_MESSAGE",
        messageSent: finalMessage,
        timestamp: new Date(),
      });

      conversation.markCompleted();
      await conversation.save();

      // Update automation stats
      await Automation.updateOne(
        { _id: conversation.automationId },
        { $inc: { "runStats.flowConversationsCompleted": 1 } }
      );

      return { success: true, completed: true, isFinal: true };
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


      const conversational = await ConversationState.findOne({
    igUserId: senderId,
    status: "active",
    expiresAt: { $gt: new Date() },
  }).sort({ startedAt: -1 });

  if (!conversational) {
    console.warn("⚠️ No conversation found for postback");
    return;
  }

  if (conversational.currentFlowId === "awaiting_initial_button_click") {
      


      if (payload === "INITIAL_DM_CLICKED") {
          console.log("✅ User clicked Initial DM Button. Starting Flow Nodes...");

          // 1. Get Tokens
          const creds = await ensureFreshPageTokenForUser(conversational.userId);
          
          // 2. Get the First Node (Follow Check)
          const firstNode = conversational.flowConfig?.[0];

          if (!firstNode) {
              console.error("❌ No flow nodes found in configuration");
              return;
          }

          // 3. Update Conversation History & State
          conversational.currentFlowId = String(firstNode.id); 
          conversational.addHistory({
              flowId: "initial_dm_click",
              flowName: "USER_CLICKED_BUTTON",
              messageSent: "Initial DM Button",
              userReply: title, 
          });
          await conversational.save();

          // 4. Execute the First Node 
          await executeFlowNode({
              flowNode: firstNode,
              conversation: conversational,
              senderId: senderId,
              pageAccessToken: creds.fbPageAccessToken,
              fbPageId: creds.fbPageId
          });

          return; // Stop here
      }
  }
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

  // DEBUG: Log what we found
  console.log("🔍 Conversation found:", {
    id: conversation._id,
    hasNestedConfig: !!conversation.currentNestedQuickReplyConfig,
    currentFlowId: conversation.currentFlowId
  });

  const nestedConfig = conversation.currentNestedQuickReplyConfig;

  if (!nestedConfig) {
    console.warn("⚠️ No nested config found in conversation");
    console.log("📋 Full conversation state:", JSON.stringify({
      currentFlowId: conversation.currentFlowId,
      currentNestedQuickReplyConfig: conversation.currentNestedQuickReplyConfig,
      historyLength: conversation.history?.length || 0
    }, null, 2));
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
          pageAccessToken: accessToken,
          fbPageId,
          parentNodeId,
        });

        if (actionResult.isNested) {
          // Another nested level - wait for user response
          return;
        }

        // FIX: If action completed successfully (like redirectLink), continue to next node
        if (actionResult.completed) {
          console.log("✅ Nested action completed");
          
          // Clear nested config
          conversation.currentNestedQuickReplyConfig = null;
          
          // Find next node after parent node
          const flowConfig = conversation.flowConfig || [];
          const nextNode = getNextFlowNode(flowConfig, parentNodeId);

          if (nextNode) {
            console.log(`→ Moving to next node: ${nextNode.id} (${nextNode.type})`);

            await executeFlowNode({
              flowNode: nextNode,
              conversation,
              senderId,
              pageAccessToken: accessToken,
              fbPageId,
              });

            console.log("✅ Next flow node executed");
            return;
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
      } catch (err) {
        console.error("❌ Failed to execute nested action:", err.message);
        conversation.markError(err);
        await conversation.save();
        return;
      }
    } else {
      // FIX: No actions on this nested option - just complete and move to next node
      console.log("ℹ️ No actions for nested option, moving to next node");
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
          pageAccessToken: accessToken,
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
  const automationId = payload.replace("FLOW_START_", "");

  const conversation = await ConversationState.findOne({
    igUserId: senderId,
    automationId,
    status: "active",
    expiresAt: { $gt: new Date() },
  }).sort({ startedAt: -1 });

   const creds = await ensureFreshPageTokenForUser(conversation.userId);
    const accessToken = creds.fbPageAccessToken;
    const fbPageId = creds.fbPageId;

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

  // Detect if buttonText action links to nested quickReply node
  if (firstNode.type === "followCheck") {
    // Usually button sends this flow
    const followingButtons = firstNode.followingButtons || [];

    // Find the button pressed, e.g. "Open Maps"
    // Assuming payload or event carries the button pressed info
    const pressedButton = followingButtons.find(btn => btn.text === title || btn.id === payload);
    if (pressedButton && pressedButton.actions && pressedButton.actions.length > 0) {
      const action = pressedButton.actions[0]; // Usually one action
      if (action.type === "quickReply") {
        // Find the nested quickReply node in flowConfig by id
        const nestedFlowNodeId = action.id || action.config?.id;
        const nestedNode = flowConfig.find((node) => String(node.id) === String(nestedFlowNodeId));

        if (!nestedNode) {
          console.error("❌ Nested quickReply node not found in flowConfig");
          return;
        }

        // Update conversation to nested quick reply node
        conversation.currentFlowId = String(nestedNode.id);
        await conversation.save();

        // Send nested quick reply node message
        await executeQuickReplyNode({
          flowNode: nestedNode,
          conversation,
          senderId,
          pageAccessToken: accessToken,
          fbPageId,
        });

        console.log("✅ Nested quick replies sent after button click");
        return;
      }
    }
  }

  // Otherwise, proceed normal flow execution for firstNode:
  try {
    await executeFlowNode({
      flowNode: firstNode,
      conversation,
      senderId,
      pageAccessToken: accessToken,
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
        pageAccessToken: accessToken,
        fbPageId,
      });

      if (result.completed) {
        console.log("✅ Follow check passed on retry. Waiting for user to click the success button.");
        // STOP HERE: The "Open Directions" button has been sent by executeFollowCheckNode.
        // We do NOT automatically move to nextNode. We wait for the FLOW_BTN_ payload.
        return;
      }
      
    } catch (err) {
      console.error("❌ FollowCheck recheck failed:", err.message);
      conversation.markError(err);
      await conversation.save();
    }

    return;
  }

  
  // ============================================================================
  // HANDLE FLOW BUTTON CLICKS (e.g., "Open Directions" inside FollowCheck)
  // ============================================================================
  if (payload.startsWith("FLOW_BTN_")) {
    console.log("→ Processing Flow Button Click");

    // Parse the payload: FLOW_BTN_{nodeId}_{buttonId}
    const parts = payload.replace("FLOW_BTN_", "").split("_");
    const nodeId = parts[0];
    const buttonId = parts[1];

    const conversation = await ConversationState.findOne({
      igUserId: senderId,
      status: "active",
      expiresAt: { $gt: new Date() },
    }).sort({ startedAt: -1 });

    if (!conversation) return;

    const flowConfig = conversation.flowConfig || [];
    const currentNode = flowConfig.find((node) => String(node.id) === String(nodeId));

    if (!currentNode) {
      console.error("❌ Node not found for button click");
      return;
    }

    // Search for the button in followingButtons (since that's where we set this up)
    // You might want to check other button arrays if you reuse this logic elsewhere
    const clickedButton = (currentNode.followingButtons || []).find(
      (btn) => String(btn.id) === String(buttonId)
    );

    if (!clickedButton) {
      console.error("❌ Button configuration not found");
      return;
    }

    const creds = await ensureFreshPageTokenForUser(conversation.userId);
    const accessToken = creds.fbPageAccessToken;
    const fbPageId = creds.fbPageId;

    // Execute the action attached to this button
    if (clickedButton.actions && clickedButton.actions.length > 0) {
      const action = clickedButton.actions[0];
      console.log(`→ Executing action for button: ${clickedButton.text}`);

      try {
        const actionResult = await executeAction({
          action,
          selectedOption: { text: clickedButton.text, id: clickedButton.id },
          conversation,
          senderId,
          pageAccessToken: accessToken,
          fbPageId,
          parentNodeId: currentNode.id,
        });

        // If the action was a Nested Quick Reply, we are done here (waiting for user input)
        if (actionResult.isNested) {
           return;
        }

        // If the action completed (e.g., it was just a link), we might want to move Next
        // usually buttons in this flow style might end here or link out, 
        // but if you want to support flow continuation after a simple button:
        if (actionResult.completed) {
           // Logic to move to next node if applicable
        }

      } catch (err) {
        console.error("❌ Failed to execute button action:", err.message);
      }
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
          pageAccessToken: accessToken,
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
          pageAccessToken: accessToken,
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