// services/realtimePublisher.js
import axios from "axios";

const REALTIME_URL = "http://34.180.49.15:3000/publish/inbox";

export async function publishInboxMessageHTTP({
  creatorId,
  conversationId,
  message
}) {
  try {
    console.log("Publishing to:", REALTIME_URL);

    await axios.post(REALTIME_URL, {
      creatorId,
      conversationId,
      message
    }, { timeout: 3000 });
  } catch (e) {
    console.error("❌ realtime publish failed", e.message);
  }
}
