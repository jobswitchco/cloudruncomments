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

  // 3️⃣ Idempotency
  const existing = await Message.findOne(
    { igMessageId },
    { _id: 1 }
  ).lean();

  let message = existing;

  if (!existing) {
    message = await Message.create({
      conversationId: conversation._id,
      platform: "instagram",
      igMessageId,

      sender: "them",
      senderType: "participant",
      senderId: participant._id,

      type,
      text,
      mediaUrl,
      mediaType,
      action,

      createdAtPlatform: createdAt,
      isRead: false,
      isDeleted: false
    });

    // 🔥 GUARDED snapshot update
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
            sender: "them",
            timestamp: createdAt
          },
          lastActivityAt: createdAt,
          lastSyncedAt: new Date()
        },
        $inc: { unreadCount: 1 }
      }
    );
  }

  return { conversation, message };
}


