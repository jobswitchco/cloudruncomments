import redis from "./redis.js";

export async function publishInboxMessage({
  creatorId,
  conversationId,
  message,
}) {
  const channel = `inbox:conversation:${conversationId}`;

  const payload = {
    creatorId,
    conversationId,
    type: "message:new",
    data: message,
    ts: Date.now(),
  };

  await redis.publish(channel, JSON.stringify(payload));
}
