// services/realtimePublisher.js
import axios from "axios";

const REALTIME_URL = "http://34.180.49.15:3000";

export async function publishInboxMessageHTTP({
  creatorId,
  conversationId,
  message,
  conversation
}) {
  try {
    console.log("Publishing to:", REALTIME_URL);

    await axios.post(`${REALTIME_URL}/publish/inbox`, {
      creatorId,
      conversationId,
      message,
      conversation
    }, { timeout: 3000,
      proxy: false
     });
  } catch (e) {
    console.error("❌ realtime publish failed", e.message);
  }
}

export async function publishConversationUpdate({
  creatorId,
  conversationId,
  update
}) {
  try {
    await axios.post(`${REALTIME_URL}/publish/conversation-update`, {
      creatorId,
      conversationId,
      update
    }, { timeout: 3000, proxy: false });
    
    console.log(`✅ Published conversation update for: ${conversationId}`);
  } catch (e) {
    console.error("❌ conversation update publish failed", e.message);
  }
}

/**
 * 🆕 Publish new conversation creation to creator's room
 */
export async function publishConversationCreated({ creatorId, conversation }) {
  try {
    const payload = {
      creatorId: String(creatorId),
      conversation: {
        _id: conversation._id.toString(),
        platform: conversation.platform,
        igConversationId: conversation.igConversationId,
        participantId: conversation.participantId.toString(),
        participant: conversation.participant, // Include populated participant data
        label: conversation.label,
        labelSource: conversation.labelSource,
        isBlocked: conversation.isBlocked,
        lastMessage: conversation.lastMessage,
        unreadCount: conversation.unreadCount,
        lastActivityAt: conversation.lastActivityAt,
        lastParticipantMessageAt: conversation.lastParticipantMessageAt || null,
        canReply: conversation.canReply,
        createdAt: conversation.createdAt,
        updatedAt: conversation.updatedAt,
      },
    };


    const res = await axios.post(`${REALTIME_URL}/publish/conversation-created`,
      payload,
       { timeout: 3000, proxy: false });

    if (res.status !== 200) {
      console.error("❌ Failed to publish conversation:created");
    } else {
      console.log("✅ Published conversation:created to creator room");
    }
  } catch (err) {
    console.error("❌ publishConversationCreated error:", err.message);
  }
}
