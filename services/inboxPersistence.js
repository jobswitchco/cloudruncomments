// services/inboxPersistence.js
import Conversation from "../models/Conversation.js";
import Message from "../models/Message.js";
import Participant from "../models/Participant.js";

export async function persistInboxMessage({
  creatorId,
  businessIgUserId,
  senderIgUserId,
  igMessageId,

  type,
  text,
  mediaUrl,
  mediaType,
  action,

  createdAt
}) {
  // 1️⃣ Ensure participant
  const participant = await Participant.findOneAndUpdate(
    { platform: "instagram", igUserId: senderIgUserId },
    { $setOnInsert: { platform: "instagram", igUserId: senderIgUserId } },
    { upsert: true, new: true }
  );

  const igConversationId = `igdm:${businessIgUserId}:${senderIgUserId}`;

  // 2️⃣ Find or create conversation
  const conversation = await Conversation.findOneAndUpdate(
    { creatorId, platform: "instagram", igConversationId },
    {
      $setOnInsert: {
        creatorId,
        platform: "instagram",
        igConversationId,
        participantId: participant._id,
        unreadCount: 0,
      },
      $set: {
        lastSyncedAt: new Date(),
      }
    },
    { upsert: true, new: true }
  );

  /* =====================================================
     🔥 CORRECT SENDER RESOLUTION (NO HARD CODING)
     ===================================================== */

  const isFromMe = senderIgUserId === businessIgUserId;

  const sender = isFromMe ? "me" : "them";
  const senderType = isFromMe ? "creator" : "participant";
  const senderTypeRef = isFromMe ? "users" : "participants";
  const senderId = isFromMe ? creatorId : participant._id;

  // 3️⃣ Create message (IDEMPOTENT)
  const existing = await Message.findOne({ igMessageId }).lean();
  if (existing) {
    return { conversation, message: existing };
  }

  const message = await Message.create({
    conversationId: conversation._id,
    platform: "instagram",
    igMessageId,

    sender,
    senderType,
    senderTypeRef,
    senderId,

    type,
    text,
    mediaUrl,
    mediaType,
    action,

    createdAtPlatform: createdAt,
    isRead: isFromMe,
    isDeleted: false
  });

  /* =====================================================
     🔥 UPDATE CONVERSATION SNAPSHOT (GUARDED)
     ===================================================== */

  await Conversation.updateOne(
    {
      _id: conversation._id,
      $or: [
        { lastActivityAt: { $exists: false } },
        { lastActivityAt: { $lt: createdAt } }
      ]
    },
    {
      $set: {
        lastMessage: {
          text,
          type,
          sender,
          timestamp: createdAt
        },
        lastActivityAt: createdAt,
        lastSyncedAt: new Date()
      },
      ...(sender === "them" ? { $inc: { unreadCount: 1 } } : {})
    }
  );

  return { conversation, message };
}



