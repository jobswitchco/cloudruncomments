// services/inboxPersistence.js
import Conversation from "../models/Conversation.js";
import Message from "../models/Message.js";
import Participant from "../models/Participant.js";

export async function persistInboxMessage({
  creatorId,
  businessIgUserId,
  senderIgUserId,
  igMessageId,
  text,
  createdAt
}) {
  // 1️⃣ Ensure participant exists
  const participant = await Participant.findOneAndUpdate(
    { platform: "instagram", igUserId: senderIgUserId },
    { $setOnInsert: { platform: "instagram", igUserId: senderIgUserId } },
    { upsert: true, new: true }
  );

  const igConversationId = `dm:${creatorId}:${senderIgUserId}`;


  // 2️⃣ Find or create conversation
const conversation = await Conversation.findOneAndUpdate(
  {
    creatorId,
    platform: "instagram",
    igConversationId
  },
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
      text,
      type: "text",
      createdAtPlatform: createdAt,
      isRead: false,
      isDeleted: false
    },
    { upsert: true, new: true }
  );

  // 4️⃣ Update conversation snapshot
  await Conversation.updateOne(
    { _id: conversation._id },
    {
      lastMessage: {
        text,
        sender: "them",
        type: "text",
        timestamp: createdAt
      },
      lastActivityAt: createdAt,
      $inc: { unreadCount: 1 }
    }
  );

  return { conversation, message };
}
