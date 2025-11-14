// index.js for cloudruncomments service
import express from "express";
import mongoose from "mongoose";
import axios from "axios";
import crypto from "crypto";
import { PubSub } from "@google-cloud/pubsub";
import qs from "qs";

import Automation from "./models/Automation.js";
import RepliedComment from "./models/RepliedComment.js";
import User from "./models/User.js";
import ActionLock from "./models/ActionLock.js";
import ConversationState from "./models/ConversationState.js";

const app = express();
const pubsub = new PubSub();

// Config
const PORT = process.env.PORT || 8080;
const PUBSUB_TOKEN = process.env.PUBSUB_TOKEN || "";
const VERIFY_TOKEN = 'CmReI394849!@349Ig987Insta0QupS';
const APP_SECRET = '2b21c578035bd7b96b24ba43e4479a52';
const META_APP_ID = "1360956302356492";
const META_APP_SECRET = "2b21c578035bd7b96b24ba43e4479a52";
const FB_API = "https://graph.facebook.com/v24.0";
const DAY_MS = 24 * 60 * 60 * 1000;

const MONGO_URI = "mongodb+srv://jobswitchco:1q2unIeMxwn9IpUB@clusterjob.5grzhlw.mongodb.net/?retryWrites=true&w=majority&appName=ClusterJob";

// Pub/Sub topics
const COMMENT_TOPIC = "ig-webhook-events";
const MESSAGING_TOPIC = "ig-messaging-events";

// ========== MIDDLEWARE ==========
// For webhook endpoint (root path), preserve raw body for HMAC verification
app.use((req, res, next) => {
  if (req.path === '/' && req.method === 'POST') {
    // Capture raw body for HMAC verification
    express.json({
      verify: (req, res, buf) => {
        req.rawBody = buf;
      }
    })(req, res, next);
  } else {
    // Regular JSON parsing for other endpoints
    express.json({ type: "*/*" })(req, res, next);
  }
});

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

function verifyMetaSignature(rawBody, signatureHeader, appSecret) {
  if (!signatureHeader || !signatureHeader.startsWith("sha256=")) return false;
  const sig = signatureHeader.slice("sha256=".length);
  const expected = crypto.createHmac("sha256", appSecret).update(rawBody).digest("hex");
  try {
    return crypto.timingSafeEqual(Buffer.from(sig, "hex"), Buffer.from(expected, "hex"));
  } catch {
    return false;
  }
}

function getEventType(parsedBody) {
  const entries = parsedBody?.entry || [];
  
  for (const entry of entries) {
    if (entry.changes && entry.changes.length > 0) {
      return "comment";
    }
    if (entry.messaging && entry.messaging.length > 0) {
      return "messaging";
    }
  }
  return "unknown";
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
  const url = `${FB_API}/${fbPageId}/messages`;
  
  const msgBody = {
    recipient: { comment_id: String(commentId) },
    message: {
      attachment: {
        type: "template",
        payload: {
          template_type: "button",
          text: message,
          buttons: buttons.map((btn) => ({
            type: "web_url",
            url: btn.url,
            title: btn.text || "Open",
          })),
        },
      },
    },
  };

  const { data, status } = await http.post(url, msgBody, {
    params: { access_token: pageAccessToken },
  });

  if (status >= 400) {
    const err = new Error("Button template failed");
    err.details = data?.error || data;
    throw err;
  }

  console.log("✅ Button template sent", commentId);
  return data;
}

async function sendQuickReplies({ fbPageId, commentId, message, quickReplies, pageAccessToken }) {
  const url = `${FB_API}/${fbPageId}/messages`;
  
  const msgBody = {
    recipient: { comment_id: String(commentId) },
    message: {
      text: message,
      quick_replies: quickReplies.map((qr) => ({
        content_type: "text",
        title: qr.title,
        payload: qr.payload,
      })),
    },
  };

  const { data, status } = await http.post(url, msgBody, {
    params: { access_token: pageAccessToken },
  });

  if (status >= 400) {
    const err = new Error("Quick replies failed");
    err.details = data?.error || data;
    throw err;
  }

  console.log("✅ Quick replies sent", commentId);
  return data;
}

async function sendGenericTemplate({ fbPageId, commentId, cards, pageAccessToken }) {
  const url = `${FB_API}/${fbPageId}/messages`;
  
  const msgBody = {
    recipient: { comment_id: String(commentId) },
    message: {
      attachment: {
        type: "template",
        payload: {
          template_type: "generic",
          elements: cards.map((card) => ({
            title: card.title,
            subtitle: card.subtitle || undefined,
            image_url: card.image_url || undefined,
            buttons: card.button
              ? [{
                  type: "web_url",
                  url: card.button.url,
                  title: card.button.text || "Open",
                }]
              : undefined,
          })),
        },
      },
    },
  };

  const { data, status } = await http.post(url, msgBody, {
    params: { access_token: pageAccessToken },
  });

  if (status >= 400) {
    const err = new Error("Generic template failed");
    err.details = data?.error || data;
    throw err;
  }

  console.log("✅ Generic template sent", commentId);
  return data;
}

async function sendFlowMessage({ fbPageId, commentId, flowNode, pageAccessToken }) {
  const { type, message, quick_replies, buttons, cards, media_url } = flowNode;

  console.log("🔄 sendFlowMessage", { type, commentId });

  switch (type) {
    case "text":
      return await sendTextMessage({ fbPageId, commentId, message, pageAccessToken });

    case "quick_replies":
      if (!quick_replies || quick_replies.length === 0) {
        throw new Error("quick_replies array is required");
      }
      return await sendQuickReplies({ fbPageId, commentId, message, quickReplies: quick_replies, pageAccessToken });

    case "button":
      if (!buttons || buttons.length === 0) {
        throw new Error("buttons array is required");
      }
      return await sendButtonTemplate({ fbPageId, commentId, message, buttons, pageAccessToken });

    case "generic":
      if (!cards || cards.length === 0) {
        throw new Error("cards array is required");
      }
      return await sendGenericTemplate({ fbPageId, commentId, cards, pageAccessToken });

    case "media":
      const url = `${FB_API}/${fbPageId}/messages`;
      const msgBody = {
        recipient: { comment_id: String(commentId) },
        message: {
          attachment: {
            type: "image",
            payload: { url: media_url },
          },
        },
      };
      const { data, status } = await http.post(url, msgBody, {
        params: { access_token: pageAccessToken },
      });
      if (status >= 400) throw new Error("Media message failed");
      return data;

    default:
      throw new Error(`Unknown flow node type: ${type}`);
  }
}

async function sendPrivateReply({ fbPageId, commentId, message, pageAccessToken, button }) {
  const hasButton = button && typeof button.url === "string" && button.url.trim();

  if (hasButton) {
    return await sendButtonTemplate({ fbPageId, commentId, message, buttons: [button], pageAccessToken });
  } else {
    return await sendTextMessage({ fbPageId, commentId, message, pageAccessToken });
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

// ---------- Message Handlers ----------
async function handlePostback(event) {
  console.log("🔘 Postback received:", { senderId: event.sender?.id, payload: event.postback?.payload });

  const senderId = event.sender?.id;
  const payload = event.postback?.payload;
  const title = event.postback?.title;

  if (!senderId || !payload) {
    console.warn("⚠️ Missing senderId or payload");
    return;
  }

  const conversation = await ConversationState.findOne({
    igUserId: senderId,
    status: "active",
    expiresAt: { $gt: new Date() },
  }).sort({ startedAt: -1 });

  if (!conversation) {
    console.log("ℹ️ No active conversation for user", senderId);
    return;
  }

  console.log("✅ Found conversation:", conversation._id.toString());

  const currentFlowId = conversation.currentFlowId;
  const flowConfig = conversation.flowConfig;
  
  const currentNode = currentFlowId === "initial"
    ? flowConfig.initial
    : flowConfig.flows?.[currentFlowId] || flowConfig.flows?.get?.(currentFlowId);

  if (!currentNode) {
    console.error("❌ Current flow node not found:", currentFlowId);
    conversation.markError(new Error(`Flow node ${currentFlowId} not found`));
    await conversation.save();
    return;
  }

  let nextFlowId;
  if (currentNode.next_actions instanceof Map) {
    nextFlowId = currentNode.next_actions.get(payload);
  } else {
    nextFlowId = currentNode.next_actions?.[payload];
  }

  console.log("🔍 Next flow:", { payload, nextFlowId });

  if (!nextFlowId) {
    console.log("🏁 End of conversation");
    conversation.markCompleted();
    await conversation.save();
    await Automation.updateOne(
      { _id: conversation.automationId },
      { $inc: { "runStats.flowConversationsCompleted": 1 } }
    );
    return;
  }

  const nextNode = flowConfig.flows?.[nextFlowId] || flowConfig.flows?.get?.(nextFlowId);

  if (!nextNode) {
    console.error("❌ Next node not found:", nextFlowId);
    conversation.markError(new Error(`Next flow ${nextFlowId} not found`));
    await conversation.save();
    return;
  }

  const creds = await ensureFreshPageTokenForUser(conversation.userId);
  const { fbPageAccessToken: accessToken, fbPageId } = creds;

  if (!accessToken || !fbPageId) {
    console.error("⚠️ Missing tokens");
    conversation.markError(new Error("Missing tokens"));
    await conversation.save();
    return;
  }

  try {
    await sendFlowMessage({
      fbPageId,
      commentId: conversation.commentId,
      flowNode: nextNode,
      pageAccessToken: accessToken,
    });

    console.log("✅ Sent next flow:", nextFlowId);

    conversation.addHistory({
      flowId: nextFlowId,
      flowName: nextFlowId,
      messageSent: nextNode.message,
      userReply: title,
      userPayload: payload,
    });

    conversation.currentFlowId = nextFlowId;
    await conversation.save();
  } catch (err) {
    console.error("❌ Failed to send next message:", err.message);
    conversation.markError(err);
    await conversation.save();
  }
}

async function handleQuickReply(event) {
  console.log("➡️ Quick reply received");
  // Quick replies come as postbacks in Instagram, so this is a fallback
  await handlePostback(event);
}

async function handleTextMessage(event) {
  const senderId = event.sender?.id;
  const text = event.message?.text;
  console.log("💬 Text message:", { senderId, text });
}

// ========== WEBHOOK ENDPOINT (ROOT) ==========
app.get("/", (req, res) => {
  console.log("GET verify request", {
    mode: req.query["hub.mode"],
    token: req.query["hub.verify_token"],
    challenge: req.query["hub.challenge"]
  });
  
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];
  
  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    console.log("✅ Webhook verified - returning challenge");
    return res.status(200).send(challenge);
  }
  
  console.warn("❌ Verification failed", {
    expectedToken: VERIFY_TOKEN,
    receivedToken: token,
    mode: mode
  });
  return res.sendStatus(403);
});

app.post("/", async (req, res) => {
  console.log("📥 Webhook POST from Meta", {
    hasRawBody: !!req.rawBody,
    rawBodyLength: req.rawBody?.length,
    hasSignature: !!req.get("X-Hub-Signature-256")
  });

  const signature = req.get("X-Hub-Signature-256");
  const raw = req.rawBody;

  if (!raw) {
    console.error("❌ No raw body available for HMAC verification");
    return res.sendStatus(400);
  }

  if (!verifyMetaSignature(raw, signature, APP_SECRET)) {
    console.warn("❌ HMAC verification failed", {
      hasSig: Boolean(signature),
      sigPrefixOk: signature?.startsWith("sha256="),
      rawLen: raw?.length || 0
    });
    return res.sendStatus(401);
  }

  console.log("✅ HMAC verified");

  const parsedBody = req.body;
  const eventType = getEventType(parsedBody);
  console.log("🎯 Event type:", eventType);

  let topic;
  if (eventType === "comment") {
    topic = COMMENT_TOPIC;
  } else if (eventType === "messaging") {
    topic = MESSAGING_TOPIC;
  } else {
    console.warn("⚠️ Unknown event type, acknowledging anyway");
    return res.sendStatus(200);
  }

  const msg = {
    receivedAt: new Date().toISOString(),
    eventType,
    headers: {
      "X-Hub-Delivery": req.get("X-Hub-Delivery") || null,
      "X-Hub-Signature-256": signature || null,
    },
    body: parsedBody,
  };

  console.log(`📤 Publishing to ${topic}`);

  try {
    await pubsub.topic(topic).publishMessage({ json: msg });
    console.log(`✅ Published to ${topic}`);
  } catch (e) {
    console.error(`❌ Pub/Sub error:`, e.message);
  }

  return res.sendStatus(200);
});

// ========== PUB/SUB ENDPOINT: COMMENTS ==========
app.post("/pubsub", async (req, res) => {
  try {
    console.log("📨 /pubsub called (comments)");

    if (PUBSUB_TOKEN) {
      const headerToken = req.get("X-Pubsub-Token");
      if (headerToken !== PUBSUB_TOKEN) {
        console.warn("⚠️ Unauthorized Pub/Sub push");
        return res.status(401).send("unauthorized");
      }
    }

    const msg = req.body?.message;
    if (!msg?.data) {
      console.log("ℹ️ Empty Pub/Sub message");
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

    console.log(`📬 Processing ${commentEvents.length} comments`);

    const userTokenCache = new Map();

    for (const c of commentEvents) {
      const autos = await Automation.find({
        platform: "instagram",
        status: "active",
        postId: c.mediaId,
      }).lean();

      if (!autos.length) {
        console.log("ℹ️ No automation for media:", c.mediaId);
        continue;
      }

      for (const auto of autos) {
        const normalizedKeywords = auto.keywords.map(normalize).filter(Boolean);
        const matched = normalizedKeywords.some((kw) => c.text.includes(kw));
        
        if (!matched) {
          console.log("ℹ️ No keyword match");
          continue;
        }

        console.log("✅ Keyword matched");

        const key = String(auto.userId);
        let creds = userTokenCache.get(key);
        if (!creds) {
          creds = await ensureFreshPageTokenForUser(auto.userId);
          userTokenCache.set(key, creds);
        }

        const accessToken = creds.fbPageAccessToken;
        const fbPageId = creds.fbPageId;

        if (!accessToken || !fbPageId) {
          console.warn("⚠️ Missing tokens for user:", key);
          continue;
        }

        let publicSent = false;
        let privateSent = false;

        // PUBLIC REPLY
        if (auto.hasPublicReply && auto.publicReply) {
          const { proceed } = await reserveAction({
            automationId: auto._id,
            commentId: c.commentId,
            channel: "public",
          });

          if (proceed) {
            try {
              await replyToCommentPublic(c.commentId, auto.publicReply, accessToken);
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
            } catch (err) {
              console.error("❌ Public reply failed", err.message);
              await finalizeAction({
                automationId: auto._id,
                commentId: c.commentId,
                channel: "public",
                ok: false,
                error: { message: err.message },
              });
            }
          }
        }

        // PRIVATE REPLY
        if (auto.dm?.enabled && c.fromUserId) {
          const { proceed } = await reserveAction({
            automationId: auto._id,
            commentId: c.commentId,
            channel: "private",
          });

          if (proceed) {
            try {
              const dmType = auto.dm.type || "simple";

              if (dmType === "conversation_flow") {
                console.log("🌊 Starting conversation flow");

                await sendFlowMessage({
                  fbPageId,
                  commentId: c.commentId,
                  flowNode: auto.dm.flowConfig.initial,
                  pageAccessToken: accessToken,
                });

                await ConversationState.create({
                  userId: auto.userId,
                  automationId: auto._id,
                  commentId: c.commentId,
                  igUserId: c.fromUserId,
                  igUsername: c.fromUsername,
                  currentFlowId: "initial",
                  flowConfig: auto.dm.flowConfig,
                  conversationHistory: [{
                    flowId: "initial",
                    flowName: "INITIAL",
                    messageSent: auto.dm.flowConfig.initial.message,
                    timestamp: new Date(),
                  }],
                  expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000),
                });

                await Automation.updateOne(
                  { _id: auto._id },
                  { $inc: { "runStats.flowConversationsStarted": 1 } }
                );
              } else {
                await sendPrivateReply({
                  fbPageId,
                  commentId: c.commentId,
                  message: auto.dm.message,
                  pageAccessToken: accessToken,
                  button: auto.dm?.button,
                });
              }

              const userDetailsUrl = `${FB_API}/${c.fromUserId}`;
              const { data: userDetails } = await axios.get(userDetailsUrl, {
                params: {
                  access_token: accessToken,
                  fields: "id,username,profile_pic,is_user_follow_business,is_business_follow_user",
                },
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
                sentMessage: dmType === "simple" ? auto.dm.message : auto.dm.flowConfig.initial.message,
                status: "sent",
                igUserId: userDetails.id,
                username: userDetails.username,
                profilePic: userDetails.profile_pic,
                followsBusiness: userDetails.is_user_follow_business,
                businessFollowsUser: userDetails.is_business_follow_user,
              });

              console.log("✅ Private reply sent");
            } catch (err) {
              console.error("❌ Private reply failed", err.message);
              await finalizeAction({
                automationId: auto._id,
                commentId: c.commentId,
                channel: "private",
                ok: false,
                error: { message: err.message },
              });
            }
          }
        }

        // Update stats
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

    return res.status(204).send();
  } catch (err) {
    console.error("❌ /pubsub error", err.message, err.stack);
    return res.status(500).send("error");
  }
});

// ========== PUB/SUB ENDPOINT: MESSAGING ==========
app.post("/pubsub-messaging", async (req, res) => {
  try {
    console.log("📨 /pubsub-messaging called");

    if (PUBSUB_TOKEN) {
      const headerToken = req.get("X-Pubsub-Token");
      if (headerToken !== PUBSUB_TOKEN) {
        console.warn("⚠️ Unauthorized Pub/Sub push");
        return res.status(401).send("unauthorized");
      }
    }

    const msg = req.body?.message;
    if (!msg?.data) {
      console.log("ℹ️ Empty Pub/Sub message");
      return res.status(204).send();
    }

    let envelope;
    try {
      const json = Buffer.from(msg.data, "base64").toString("utf8");
      envelope = JSON.parse(json);
      console.log("📨 Decoded messaging event");
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
        if (event.postback) {
          await handlePostback(event);
          continue;
        }

        if (event.message?.quick_reply) {
          await handleQuickReply(event);
          continue;
        }

        if (event.message) {
          await handleTextMessage(event);
        }

        if (event.reaction) {
          console.log("👍 Reaction:", event.reaction);
        }
      }
    }

    return res.status(204).send();
  } catch (err) {
    console.error("❌ /pubsub-messaging error", err.message, err.stack);
    return res.status(500).send("error");
  }
});

// Health check
app.get("/health", (_req, res) => res.status(200).send("ok"));

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Server listening on port ${PORT} at ${new Date().toISOString()}`);
});
