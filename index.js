import express from "express";
import mongoose from "mongoose";
import axios from "axios";
import qs from "qs";

import Automation from "./models/Automation.js";
import RepliedComment from "./models/RepliedComment.js";
import User from "./models/User.js";
import ActionLock from "./models/ActionLock.js";
import ConversationState from "./models/ConversationState.js";

const username = "jobswitchco";
const password = "1q2unIeMxwn9IpUB";
const MONGO_URI =
  "mongodb+srv://" +
  username +
  ":" +
  password +
  "@clusterjob.5grzhlw.mongodb.net/?retryWrites=true&w=majority&appName=ClusterJob";

const PORT = 8080;
const PUBSUB_TOKEN = "";

const app = express();
app.use(express.json({ type: "*/*" }));

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
      const commentId = v.id;
      const mediaId = v.media?.id;
      const text = v.text || "";
      const fromUserId = v.from?.id;
      const fromUsername = v.from?.username;

      events.push({
        eventId: envelope?.headers?.["X-Hub-Delivery"] || commentId || Math.random().toString(36),
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
      console.error("⚠️ FB refresh failed:", e?.response?.data || e.message || e);
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
    console.error("❌ IG reply failed", commentId, err.response?.data || err.message);
    throw err;
  }
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

  console.log("-> sendButtonTemplate payload:", JSON.stringify(msgBody, null, 2));

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

  console.log("-> sendQuickReplies payload:", JSON.stringify(msgBody, null, 2));

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
              ? [
                  {
                    type: "web_url",
                    url: card.button.url,
                    title: card.button.text || "Open",
                  },
                ]
              : undefined,
          })),
        },
      },
    },
  };

  console.log("-> sendGenericTemplate payload:", JSON.stringify(msgBody, null, 2));

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

  switch (type) {
    case "text":
      return await sendTextMessage({ fbPageId, commentId, message, pageAccessToken });

    case "quick_replies":
      if (!quick_replies || quick_replies.length === 0) {
        throw new Error("quick_replies array is required for type quick_replies");
      }
      return await sendQuickReplies({
        fbPageId,
        commentId,
        message,
        quickReplies: quick_replies,
        pageAccessToken,
      });

    case "button":
      if (!buttons || buttons.length === 0) {
        throw new Error("buttons array is required for type button");
      }
      return await sendButtonTemplate({
        fbPageId,
        commentId,
        message,
        buttons,
        pageAccessToken,
      });

    case "generic":
      if (!cards || cards.length === 0) {
        throw new Error("cards array is required for type generic");
      }
      return await sendGenericTemplate({
        fbPageId,
        commentId,
        cards,
        pageAccessToken,
      });

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
    return await sendButtonTemplate({
      fbPageId,
      commentId,
      message,
      buttons: [button],
      pageAccessToken,
    });
  } else {
    return await sendTextMessage({
      fbPageId,
      commentId,
      message,
      pageAccessToken,
    });
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

// ========== PUB/SUB ENDPOINT 1: COMMENTS (EXISTING) ==========
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
              console.log("✅ Public reply sent", { commentId: c.commentId });
            } catch (err) {
              console.error("❌ Public reply failed", err.details || err.message);
              await finalizeAction({
                automationId: auto._id,
                commentId: c.commentId,
                channel: "public",
                ok: false,
                error: err.details || { message: err.message },
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
              let data;
              const dmType = auto.dm.type || "simple";

              if (dmType === "conversation_flow") {
                console.log("🌊 Starting conversation flow for comment", c.commentId);

                data = await sendFlowMessage({
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
                  conversationHistory: [
                    {
                      flowId: "initial",
                      flowName: "INITIAL",
                      messageSent: auto.dm.flowConfig.initial.message,
                      timestamp: new Date(),
                    },
                  ],
                  expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000),
                });

                console.log("✅ Conversation flow started", c.commentId);

                await Automation.updateOne(
                  { _id: auto._id },
                  { $inc: { "runStats.flowConversationsStarted": 1 } }
                );
              } else {
                data = await sendPrivateReply({
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

              console.log("✅ Private reply + user details saved");
            } catch (err) {
              console.error("❌ Private reply failed", err.details || err.message);
              await finalizeAction({
                automationId: auto._id,
                commentId: c.commentId,
                channel: "private",
                ok: false,
                error: err.details || { message: err.message },
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
    console.error("❌ Unhandled /pubsub error", err.message);
    return res.status(500).send("error");
  }
});

// ========== PUB/SUB ENDPOINT 2: MESSAGING (NEW) ==========
app.post("/pubsub-messaging", async (req, res) => {
  try {
    if (PUBSUB_TOKEN) {
      const headerToken = req.get("X-Pubsub-Token");
      if (headerToken !== PUBSUB_TOKEN) {
        console.warn("⚠️ Unauthorized Pub/Sub push (messaging)");
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
      console.error("❌ Pub/Sub decode failed (messaging)", e);
      return res.status(204).send();
    }

    console.log("📨 Processing messaging event:", JSON.stringify(envelope, null, 2));

    await connectMongo();

    const entries = envelope?.body?.entry || [];
    
    for (const entry of entries) {
      const messaging = entry?.messaging || [];
      
      for (const event of messaging) {
        if (event.postback) {
          await handlePostback(event);
        }
        
        if (event.message && !event.message.quick_reply) {
          await handleTextMessage(event);
        }
        
        if (event.reaction) {
          console.log("👍 Reaction received:", event.reaction);
        }
      }
    }

    return res.status(204).send();
  } catch (err) {
    console.error("❌ Unhandled /pubsub-messaging error", err.message);
    return res.status(500).send("error");
  }
});

async function handlePostback(event) {
  const senderId = event.sender?.id;
  const payload = event.postback?.payload;
  const title = event.postback?.title;

  console.log("🔘 Postback received", { senderId, payload, title });

  if (!senderId || !payload) {
    console.warn("⚠️ Missing senderId or payload in postback");
    return;
  }

  const conversation = await ConversationState.findOne({
    igUserId: senderId,
    status: "active",
    expiresAt: { $gt: new Date() },
  }).sort({ startedAt: -1 });

  if (!conversation) {
    console.log("ℹ️ No active conversation found for user", senderId);
    return;
  }

  console.log("✅ Found active conversation:", conversation._id);

  const currentFlowId = conversation.currentFlowId;
  const flowConfig = conversation.flowConfig;
  
  const currentNode =
    currentFlowId === "initial"
      ? flowConfig.initial
      : flowConfig.flows?.[currentFlowId];

  if (!currentNode) {
    console.error("❌ Current flow node not found:", currentFlowId);
    conversation.markError(new Error(`Flow node ${currentFlowId} not found`));
    await conversation.save();
    return;
  }

  const nextFlowId = currentNode.next_actions?.[payload];

  if (!nextFlowId) {
    console.log("🏁 End of conversation - no next flow for payload:", payload);
    conversation.markCompleted();
    await conversation.save();

    await Automation.updateOne(
      { _id: conversation.automationId },
      { $inc: { "runStats.flowConversationsCompleted": 1 } }
    );

    return;
  }

  const nextNode = flowConfig.flows?.[nextFlowId];

  if (!nextNode) {
    console.error("❌ Next flow node not found:", nextFlowId);
    conversation.markError(new Error(`Next flow ${nextFlowId} not found`));
    await conversation.save();
    return;
  }

  const creds = await ensureFreshPageTokenForUser(conversation.userId);
  const accessToken = creds.fbPageAccessToken;
  const fbPageId = creds.fbPageId;

  if (!accessToken || !fbPageId) {
    console.error("⚠️ Missing tokens for user", conversation.userId);
    return;
  }

  try {
    await sendFlowMessage({
      fbPageId,
      commentId: conversation.commentId,
      flowNode: nextNode,
      pageAccessToken: accessToken,
    });

    console.log("✅ Sent next flow message:", nextFlowId);

    conversation.addHistory({
      flowId: nextFlowId,
      flowName: nextFlowId,
      messageSent: nextNode.message,
      userReply: title,
      userPayload: payload,
    });

    conversation.currentFlowId = nextFlowId;
    await conversation.save();

    console.log("✅ Conversation state updated");
  } catch (err) {
    console.error("❌ Failed to send next message:", err);
    conversation.markError(err);
    await conversation.save();
  }
}

async function handleTextMessage(event) {
  const senderId = event.sender?.id;
  const text = event.message?.text;

  console.log("💬 Text message received", { senderId, text });
}

// ---------- Health ----------
app.get("/", (_req, res) => res.status(200).send("ok"));

// ---------- Start ----------
app.listen(PORT, () => {
  console.log(`🚀 Worker listening on ${PORT} at ${new Date().toISOString()}`);
});
