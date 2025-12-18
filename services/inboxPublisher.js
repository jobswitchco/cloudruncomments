// services/inboxPublisher.js
import redis from "./redis.js";

export async function publishInboxMessage({
  creatorId,
  conversationId,
  message
}) {
  await redis.publish(
    `inbox:conversation:${conversationId}`,
    JSON.stringify({
      type: "message:new",
      conversationId,
      message
    })
  );

  await redis.hincrby(
    `inbox:unread:${creatorId}`,
    conversationId,
    1
  );

  await redis.zadd(
    `inbox:conversations:${creatorId}`,
    Date.now(),
    conversationId
  );
}
