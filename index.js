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
const PORT = process.env.PORT || 8080;
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



// ---------- Robust sendButtonTemplate + sendPrivateReply ----------

async function sendButtonTemplate({ fbPageId, commentId, message, buttons, pageAccessToken }) {
  if (!Array.isArray(buttons) || buttons.length === 0) {
    throw new Error("buttons array is required");
  }

  // Normalize & validate: allow up to 3, require https for web_url
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

  // Try API call (single attempt here — caller can retry) and inspect error closely
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
    // Normalize error details shape
    const details = err.details || err.response?.data || (err.message ? { message: err.message } : err);
    console.error("⚠️ sendButtonTemplate error:", JSON.stringify(details, null, 2));

    // If this is the 'already has a reply' case, surface that info
    const subcode = details?.error_subcode || details?.error?.error_subcode || details?.code;
    if (subcode === 2534023 || (details?.message && /already has a reply/i.test(details.message))) {
      const noteworthy = new Error("Comment already has a reply (2534023)");
      noteworthy.details = details;
      noteworthy.code = 2534023;
      throw noteworthy;
    }

    // For any other error, throw so caller handles fallback/retry
    const e = new Error("Button template failed");
    e.details = details;
    throw e;
  }
}

async function sendPrivateReply({ fbPageId, commentId, message, pageAccessToken, button }) {
  const hasButton = button && typeof button.url === "string" && button.url.trim();

  if (!hasButton) {
    // Simple path
    return await sendTextMessage({ fbPageId, commentId, message, pageAccessToken });
  }

  // If a button is requested, try template; if it errors with "already has a reply", fallback to plain text
  try {
    const resp = await sendButtonTemplate({ fbPageId, commentId, message, buttons: [button], pageAccessToken });
    return resp;
  } catch (err) {
    // Known: comment already has a reply (2534023). We'll attempt a text fallback (message + explicit URL)
    if (err.code === 2534023 || (err.details && err.details.error_subcode === 2534023)) {
      console.warn("⚠️ Button template rejected because comment already has a reply. Attempting text fallback.", JSON.stringify(err.details || err.message || err, null, 2));
      // Fallback message includes the link so user can still act
      const fallbackMsg = message ? `${message}\n\nOpen here: ${button.url}` : `Open here: ${button.url}`;

      try {
        const txt = await sendTextMessage({ fbPageId, commentId, message: fallbackMsg, pageAccessToken });
        console.log("✅ Fallback text (after 2534023) succeeded", { commentId });
        return { ok: true, data: txt, fallback: true, reason: "already_has_reply" };
      } catch (tErr) {
        console.error("❌ Fallback text after 2534023 failed:", JSON.stringify(tErr?.response?.data || tErr?.message || tErr, null, 2));
        const re = new Error("Button template failed; fallback text also failed");
        re.details = { buttonError: err.details || err.message, fallbackError: tErr?.response?.data || tErr?.message };
        throw re;
      }
    }

    // For other errors, try a normal text fallback as well (to be defensive)
    console.warn("⚠️ sendButtonTemplate failed (non-2534023). Will try text fallback as a best effort.", JSON.stringify(err.details || err.message || err, null, 2));
    const fallbackMsg = message ? `${message}\n\nOpen here: ${button.url}` : `Open here: ${button.url}`;
    try {
      const txt = await sendTextMessage({ fbPageId, commentId, message: fallbackMsg, pageAccessToken });
      console.log("✅ Fallback text (after other button error) succeeded", { commentId });
      return { ok: true, data: txt, fallback: true, reason: "button_error" };
    } catch (tErr) {
      console.error("❌ Fallback text after button error failed:", JSON.stringify(tErr?.response?.data || tErr?.message || tErr, null, 2));
      const re = new Error("Button template failed; fallback text also failed");
      re.details = { buttonError: err.details || err.message, fallbackError: tErr?.response?.data || tErr?.message };
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

// ---------- Message Handlers ----------
async function handlePostback(event) {
  console.log("🔘 Full postback event:", JSON.stringify(event, null, 2));
  
  const senderId = event.sender?.id;
  
  // Instagram quick replies can come in two formats:
  // 1. event.postback.payload (button clicks)
  // 2. event.message.quick_reply.payload (quick reply clicks)
  let payload;
  let title;
  
  if (event.postback) {
    payload = event.postback.payload;
    title = event.postback.title;
  } else if (event.message?.quick_reply) {
    payload = event.message.quick_reply.payload;
    title = event.message.text;
  }

  console.log("🔍 Extracted:", { senderId, payload, title });

  if (!senderId || !payload) {
    console.warn("⚠️ Missing senderId or payload", { 
      hasSenderId: !!senderId, 
      hasPayload: !!payload,
      eventKeys: Object.keys(event)
    });
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

  console.log("🔍 Next flow lookup:", { 
    payload, 
    nextFlowId, 
    availableActions: Object.keys(currentNode.next_actions || {})
  });

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
    console.error("❌ Failed to send next message:", err.message, err.stack);
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
  console.log("🔍 Event structure:", {
    hasPostback: !!event.postback,
    hasQuickReply: !!event.message?.quick_reply,
    hasMessage: !!event.message,
    hasReaction: !!event.reaction,
    eventKeys: Object.keys(event)
  });

  // Handle postbacks (button clicks)
  if (event.postback) {
    await handlePostback(event);
    continue;
  }

  // Handle quick reply responses (they come as messages with quick_reply field)
  if (event.message?.quick_reply) {
    console.log("➡️ Quick reply detected, routing to handlePostback");
    await handlePostback(event); // Route to same handler
    continue;
  }

  // Handle regular text messages
  if (event.message && !event.message.quick_reply) {
    await handleTextMessage(event);
  }

  // Handle reactions
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

// Health check (simple GET endpoint)
app.get("/", (_req, res) => res.status(200).send("ok"));
app.get("/health", (_req, res) => res.status(200).send("ok"));

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Pub/Sub processor listening on port ${PORT} at ${new Date().toISOString()}`);
});
