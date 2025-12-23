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
