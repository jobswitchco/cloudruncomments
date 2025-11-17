// index.js for cloudruncomments service - PUBSUB PROCESSOR ONLY
import express from "express";
import mongoose from "mongoose";
import axios from "axios";
import qs from "qs";

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

// ---------- Action Locking ----------
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

    if (doc.state !== "reserved" || !doc.reservedAt) return { proceed: false, doc };
    return { proceed: true, doc };
  } catch (e) {
    if (e?.code === 11000) return { proceed: false, error: e };
    throw e;
  }
}

async function finalizeAction({ automationId, commentId, channel, ok, error }) {
  const update = ok
    ? { state: "sent", sentAt: new Date(), error: undefined }
    : { state: "failed", error };

  await ActionLock.updateOne({ automationId, commentId, channel }, { $set: update });
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
        // Keyword matching updated
        const normalizedKeywords = (auto.keywords || []).map(normalize).filter(Boolean);
        const matched = normalizedKeywords.length === 0 || normalizedKeywords.some((kw) => c.text.includes(kw));
        if (!matched) {
          console.log("ℹ️ No keyword match for comment:", c.text);
          continue;
        }

        // Fetch tokens caching
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

        // Public reply only if hasReply = true and replyComment provided
        if (auto.hasReply && auto.replyComment) {
          const { proceed } = await reserveAction({
            automationId: auto._id,
            commentId: c.commentId,
            channel: "public",
          });

          if (proceed) {
            try {
              await replyToCommentPublic(c.commentId, auto.replyComment, accessToken);
              console.log("✅ Public reply sent for comment:", c.commentId);

              await finalizeAction({
                automationId: auto._id,
                commentId: c.commentId,
                channel: "public",
                ok: true,
              });
            } catch (err) {
              console.error("❌ Public reply failed:", err.message);
              await finalizeAction({
                automationId: auto._id,
                commentId: c.commentId,
                channel: "public",
                ok: false,
                error: { message: err.message },
              });
              continue; // Skip DM if public reply failed
            }
          } else {
            console.log("ℹ️ Public action already done or reserved for comment:", c.commentId);
          }
        }

        // Private one-time DM sending
        const { proceed: canSendPrivate } = await reserveAction({
          automationId: auto._id,
          commentId: c.commentId,
          channel: "private",
        });

if (canSendPrivate && auto.dmMessage && auto.buttonText) {
  try {
    await sendInitialDMWithQuickReplies({
      fbPageId,
      commentId: c.commentId,
      automation: auto,
      pageAccessToken: accessToken,
      igUserId: c.fromUserId,
      igUsername: c.fromUsername,
    });

    // Fetch and save user details
    const userDetailsUrl = `${FB_API}/${c.fromUserId}`;
    const { data: userDetails } = await axios.get(userDetailsUrl, {
      params: {
        access_token: accessToken,
        fields: "id,username,profile_pic,is_user_follow_business,is_business_follow_user",
      },
    });

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
      commentId: c.commentId,
      channel: "private",
      ok: true,
    });

    console.log("✅ DM + QuickReply sent successfully");
  } catch (err) {
    console.error("❌ Failed:", err.message);
    await finalizeAction({
      automationId: auto._id,
      commentId: c.commentId,
      channel: "private",
      ok: false,
      error: { message: err.message },
    });
  }
}


 else {
          if (!canSendPrivate) {
            console.log("ℹ️ Private action already done or reserved for comment:", c.commentId);
          } else {
            console.log("ℹ️ dmMessage or buttonText missing or DM disabled for automation:", auto._id);
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


async function sendInitialDMWithQuickReplies({
  fbPageId,
  commentId,
  automation,
  pageAccessToken,
  igUserId,
  igUsername,
}) {
  try {
    // STEP 1: Send button message
    const buttonPayload = {
      type: "postback",
      title: automation.buttonText,
      payload: `FLOW_START_${automation._id}`,
    };

    const url = `${FB_API}/${fbPageId}/messages`;
    const buttonBody = {
      recipient: { comment_id: commentId },
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

    const { data: btnData, status: btnStatus } = await axios.post(
      url,
      buttonBody,
      { params: { access_token: pageAccessToken } }
    );

    if (btnStatus >= 400) {
      throw new Error(`Button send failed: ${JSON.stringify(btnData)}`);
    }

    console.log("✅ Button message sent");

    // STEP 2: Check first node type
    const firstNode = automation.flowNodes?.[0];

    if (firstNode && firstNode.type === "quickReply") {
      // Wait before sending quick replies
      await new Promise((resolve) => setTimeout(resolve, 1500));

      // STEP 3: Send quick replies
      const quickReplies = (firstNode.replyOptions || [])
        .slice(0, 13)
        .map((option) => ({
          content_type: "text",
          title: (option.text || "Option").toString().slice(0, 20),
          payload: `QR_${firstNode.id}_${option.id}`,
        }));

      if (quickReplies.length > 0) {
        const qrBody = {
          recipient: { comment_id: commentId },
          message: {
            text: firstNode.config?.quickReplyQuestion || "Choose one:",
            quick_replies: quickReplies,
          },
        };

        const { data: qrData, status: qrStatus } = await axios.post(
          url,
          qrBody,
          { params: { access_token: pageAccessToken } }
        );

        if (qrStatus >= 400) {
          console.warn("⚠️ Quick replies send failed:", qrData);
          // Don't throw - button was sent successfully
        } else {
          console.log("✅ Quick replies sent");
        }
      }
    }

    // STEP 4: Create ConversationState
    await ConversationState.create({
      userId: automation.userId,
      automationId: automation._id,
      commentId: commentId,
      igUserId: igUserId,
      igUsername: igUsername,
      currentFlowId: String(firstNode?.id),
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

    return { ok: true };
  } catch (err) {
    console.error("❌ Error in sendInitialDMWithQuickReplies:", err.message);
    throw err;
  }
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
// UPDATED: handlePostback Function (For Quick Replies and Regular Buttons)
// ============================================================================
// This function now handles:
// 1. Quick reply payloads from quickReply nodes
// 2. Postback button payloads from button nodes
// 3. FollowCheck verification
// 4. Flow continuation

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

  const conversation = await ConversationState.findOne({
    igUserId: senderId,
    status: "active",
    expiresAt: { $gt: new Date() },
  }).sort({ startedAt: -1 });

  if (!conversation) {
    console.log("ℹ️ No active conversation found for user:", senderId);
    return;
  }

  const currentFlowId = conversation.currentFlowId;
  const flowConfig = conversation.flowConfig || [];

  const currentNode = flowConfig.find((node) => String(node.id) === String(currentFlowId));

  if (!currentNode) {
    console.error("❌ Current node not found:", currentFlowId);
    return;
  }

  // ============================================================================
  // HANDLE QUICKREPLY NODE TYPE
  // ============================================================================
  if (currentNode.type === "quickReply") {
    console.log("→ Processing Quick Reply from quickReply node");

    // Find which option was selected
    const selectedOption = currentNode.replyOptions?.find(
      (opt) =>
        payload === `QR_${currentNode.id}_${opt.id}` ||
        payload === `QR_${currentNode.id}_${currentNode.replyOptions.indexOf(opt)}`
    );

    if (!selectedOption) {
      console.warn("⚠️ Selected option not found for payload:", payload);
      return;
    }

    console.log("✅ User selected:", selectedOption.text);

    // Add to history
    conversation.addHistory({
      flowId: String(currentNode.id),
      flowName: "QUICK_REPLY",
      messageSent: currentNode.config.quickReplyQuestion,
      userReply: selectedOption.text,
      userPayload: payload,
    });

    // ✅ Check if this option has actions
    if (selectedOption.actions && selectedOption.actions.length > 0) {
      const action = selectedOption.actions[0];

      const creds = await ensureFreshPageTokenForUser(conversation.userId);
      const accessToken = creds.fbPageAccessToken;
      const fbPageId = creds.fbPageId;

      if (!accessToken || !fbPageId) {
        console.error("❌ Missing credentials");
        conversation.markError(new Error("Missing credentials"));
        await conversation.save();
        return;
      }

      try {
        // Execute the action (e.g., redirect link)
        if (action.type === "redirectLink") {
          const redirectUrl = action.config?.redirectUrl || "https://example.com";
          const instagramPage = action.config?.instagramPage || "";

          console.log("→ Executing redirect link action:", redirectUrl);

          // Send message with button containing the redirect URL
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
            pageAccessToken: accessToken,
            fbPageId,
          });

          console.log("✅ Redirect action sent");
        } else if (action.type === "nextFlow") {
          // Move to next flow node if action specifies it
          const nextFlowId = action.config?.nextFlowId;
          if (nextFlowId) {
            const nextNode = flowConfig.find((node) => node.id === nextFlowId);
            if (nextNode) {
              conversation.currentFlowId = nextFlowId;

              try {
                await sendFlowMessage({
                  recipient: { id: senderId },
                  flowNode: nextNode,
                  pageAccessToken: accessToken,
                  fbPageId,
                });

                console.log("✅ Next flow node sent");
              } catch (err) {
                console.error("❌ Failed to send next flow node:", err.message);
                conversation.markError(err);
                await conversation.save();
                return;
              }
            }
          }
        }
      } catch (err) {
        console.error("❌ Failed to execute action:", err.message);
        conversation.markError(err);
        await conversation.save();
        return;
      }
    } else {
      // No actions - just mark this step and move to next or complete
      console.log("ℹ️ No actions configured for this option");
    }

    // Mark conversation completed if no next node
    conversation.markCompleted();
    await conversation.save();
    await Automation.updateOne(
      { _id: conversation.automationId },
      { $inc: { "runStats.flowConversationsCompleted": 1 } }
    );

    return;
  }

  // ============================================================================
  // HANDLE FOLLOWCHECK NODE TYPE
  // ============================================================================
  if (currentNode.type === "followCheck") {
    console.log("→ Processing FollowCheck verification");

    const creds = await ensureFreshPageTokenForUser(conversation.userId);
    const accessToken = creds.fbPageAccessToken;
    const fbPageId = creds.fbPageId;

    if (!accessToken || !fbPageId) {
      console.error("❌ Missing credentials");
      return;
    }

    // Fetch user follow status from Instagram API
    const userDetailsUrl = `${FB_API}/${senderId}`;
    let userDetails;
    try {
      const response = await axios.get(userDetailsUrl, {
        params: {
          access_token: accessToken,
          fields: "id,username,profile_pic,is_user_follow_business,is_business_follow_user",
        },
      });
      userDetails = response.data;
    } catch (err) {
      console.error("❌ Failed to fetch user follow status:", err.message);
      userDetails = { is_user_follow_business: false };
    }

    const isFollowing = userDetails.is_user_follow_business === true;

    console.log("🔍 Follow Status Check:", {
      userId: senderId,
      isFollowing,
    });

    // ========== USER IS FOLLOWING ==========
    if (isFollowing) {
      console.log("✅ User is following! Proceeding with following branch...");

      const followingButtons = currentNode.followingButtons || [];

      if (followingButtons.length === 0) {
        console.warn("⚠️ No following buttons configured");
        conversation.markCompleted();
        await conversation.save();
        return;
      }

      const followingButton = followingButtons[0];

      if (followingButton.actions && followingButton.actions.length > 0) {
        const action = followingButton.actions[0];

        try {
          await sendFlowMessage({
            recipient: { id: senderId },
            flowNode: {
              type: "button",
              message: currentNode.config.followCheckYesMessage,
              buttons: [
                {
                  type: "web_url",
                  title: followingButton.text,
                  url: action.config?.redirectUrl || "https://example.com",
                },
              ],
            },
            pageAccessToken: accessToken,
            fbPageId,
          });

          console.log("✅ Following branch button sent");
        } catch (err) {
          console.error("❌ Failed to send following button:", err.message);
        }
      } else {
        try {
          await sendFlowMessage({
            recipient: { id: senderId },
            flowNode: {
              type: "text",
              message: currentNode.config.followCheckYesMessage,
            },
            pageAccessToken: accessToken,
            fbPageId,
          });

          console.log("✅ Following branch message sent");
        } catch (err) {
          console.error("❌ Failed to send message:", err.message);
        }
      }

      conversation.addHistory({
        flowId: String(currentNode.id),
        flowName: "FOLLOW_CHECK_SUCCESS",
        messageSent: currentNode.config.followCheckYesMessage,
        userReply: "Following confirmed",
        userPayload: "FOLLOWING_VERIFIED",
      });

      conversation.markCompleted();
      await conversation.save();
      await Automation.updateOne(
        { _id: conversation.automationId },
        { $inc: { "runStats.flowConversationsCompleted": 1 } }
      );

      return;
    }

    // ========== USER IS NOT FOLLOWING ==========
    else {
      console.log("❌ User not following. Showing verification button again...");

      const notFollowingButtons = currentNode.notFollowingButtons || [];

      if (notFollowingButtons.length === 0) {
        console.warn("⚠️ No notFollowing buttons configured");
        conversation.markCompleted();
        await conversation.save();
        return;
      }

      const verificationButton = notFollowingButtons[0];
      const verificationPayload = `FOLLOWCHECK_RECHECK_${currentNode.id}`;

      try {
        await sendFlowMessage({
          recipient: { id: senderId },
          flowNode: {
            type: "button",
            message: currentNode.config.followCheckNoMessage,
            buttons: [
              {
                type: "postback",
                title: verificationButton.text,
                payload: verificationPayload,
              },
            ],
          },
          pageAccessToken: accessToken,
          fbPageId,
        });

        console.log("✅ Verification button sent again");
      } catch (err) {
        console.error("❌ Failed to send verification button:", err.message);
      }

      conversation.addHistory({
        flowId: String(currentNode.id),
        flowName: "FOLLOW_CHECK_RETRY",
        messageSent: currentNode.config.followCheckNoMessage,
        userReply: "Not following, retrying",
        userPayload: verificationPayload,
      });

      // Keep active for retry
      await conversation.save();

      return;
    }
  }

  // ============================================================================
  // HANDLE STANDARD FLOW NODES
  // ============================================================================

  const nextFlowId =
    currentNode.next_actions instanceof Map
      ? currentNode.next_actions.get(payload)
      : currentNode.next_actions?.[payload];

  if (!nextFlowId) {
    console.log("🏁 Conversation completed (no next flow)");
    conversation.markCompleted();
    await conversation.save();
    await Automation.updateOne(
      { _id: conversation.automationId },
      { $inc: { "runStats.flowConversationsCompleted": 1 } }
    );
    return;
  }

  const nextNode = flowConfig.find((node) => node.id === nextFlowId);
  if (!nextNode) {
    console.error("❌ Next node not found:", nextFlowId);
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
    await sendFlowMessage({
      recipient: { id: String(senderId) },
      flowNode: nextNode,
      pageAccessToken: accessToken,
      fbPageId,
    });

    conversation.addHistory({
      flowId: nextFlowId,
      flowName: nextFlowId,
      messageSent: nextNode.message,
      userReply: title,
      userPayload: payload,
    });

    conversation.currentFlowId = nextFlowId;
    await conversation.save();

    console.log("✅ Next flow node sent");
  } catch (err) {
    console.error("❌ Failed to send next node:", err.message, err.details || err.stack);
    conversation.markError(err);
    await conversation.save();
  }
}




// Health check
app.get("/", (_req, res) => res.status(200).send("ok"));
app.get("/health", (_req, res) => res.status(200).send("ok"));

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Pub/Sub processor listening on port ${PORT} at ${new Date().toISOString()}`);
});
