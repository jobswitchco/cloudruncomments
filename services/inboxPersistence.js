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

  createdAt,
  skipIfNoConversation = false, // NEW FLAG for discovery flow
}) {
  // =========================================================
  // 1️⃣ Ensure participant exists
  // =========================================================
  const participant = await Participant.findOneAndUpdate(
    { platform: "instagram", igUserId: senderIgUserId },
    { $setOnInsert: { platform: "instagram", igUserId: senderIgUserId } },
    { upsert: true, new: true }
  );

  const igConversationId = `igdm:${businessIgUserId}:${senderIgUserId}`;

  // =========================================================
  // 2️⃣ Find existing conversation
  // =========================================================
  let conversation = await Conversation.findOne({
    creatorId,
    platform: "instagram",
    igConversationId,
  });

  // =========================================================
  // 🔥 NEW: If conversation doesn't exist and skipIfNoConversation is true
  // Return null to signal that conversation discovery is needed
  // =========================================================
  if (!conversation && skipIfNoConversation) {
    console.log("ℹ️ Conversation not found, signaling for discovery");
    return null;
  }

  // =========================================================
  // 3️⃣ Create conversation if it doesn't exist (normal flow)
  // =========================================================
  if (!conversation) {
    conversation = await Conversation.create({
      creatorId,
      platform: "instagram",
      igConversationId,
      participantId: participant._id,
      unreadCount: 0,
      lastSyncedAt: new Date(),
      lastActivityAt: createdAt,
      label: "General",
      labelSource: "auto",
    });
    console.log("✅ New conversation created:", conversation._id);
  }

  // =========================================================
  // 4️⃣ Resolve sender correctly (NO HARD CODING)
  // =========================================================
  const isFromMe = senderIgUserId === businessIgUserId;

  const sender = isFromMe ? "me" : "them";
  const senderType = isFromMe ? "creator" : "participant";
  const senderTypeRef = isFromMe ? "users" : "participants";
  const senderId = isFromMe ? creatorId : participant._id;

  // =========================================================
  // 5️⃣ Create message (IDEMPOTENT - skip if already exists)
  // =========================================================
  const existing = await Message.findOne({ igMessageId }).lean();
  if (existing) {
    console.log("ℹ️ Message already exists:", igMessageId);
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
    isRead: isFromMe, // Creator's own messages are always read
    isDeleted: false,
  });

  console.log("✅ Message created:", message._id);

  // =========================================================
  // 6️⃣ Update conversation snapshot (GUARDED)
  // Only update if this message is newer than lastActivityAt
  // =========================================================
  const update = {
    lastMessage: {
      text: text || (type === "image" ? "Sent an image" : type === "video" ? "Sent a video" : "Sent a message"),
      type,
      sender,
      timestamp: createdAt,
    },
    lastActivityAt: createdAt,
    lastSyncedAt: new Date(),
  };

  // 🔥 CRITICAL: Only update lastParticipantMessageAt if sender is "them"
  if (sender === "them") {
    update.lastParticipantMessageAt = createdAt;
  }

  // Build update operation
  const updateOperation = {
    $set: update,
  };

  // Increment unread count only if message is from participant
  if (sender === "them") {
    updateOperation.$inc = { unreadCount: 1 };
  }

  // Update conversation only if this message is newer (or if lastActivityAt doesn't exist)
  const updatedConversation = await Conversation.findOneAndUpdate(
    {
      _id: conversation._id,
      $or: [
        { lastActivityAt: { $exists: false } },
        { lastActivityAt: { $lt: createdAt } },
      ],
    },
    updateOperation,
    { new: true }
  );

  // If update didn't match (message was older), return original conversation
  const finalConversation = updatedConversation || conversation;

  console.log("✅ Conversation updated:", {
    id: finalConversation._id,
    unreadCount: finalConversation.unreadCount,
    lastActivityAt: finalConversation.lastActivityAt,
  });

  return {
    conversation: finalConversation,
    message,
  };
}