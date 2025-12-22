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
        lastActivityAt: createdAt,
        lastSyncedAt: new Date(),
      }
    },
    { upsert: true, new: true }
  );

  // 3️⃣ Upsert message (IDEMPOTENT)
  const message = await Message.findOneAndUpdate(
    { igMessageId },
    {
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
    },
    { upsert: true, new: true }
  );

  // 4️⃣ Update conversation snapshot ONLY on insert
  if (message.wasNew || message.lastErrorObject?.upserted) {
    await Conversation.updateOne(
      { _id: conversation._id },
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

