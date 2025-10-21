const express = require("express");
const app = express();

// Accept Pub/Sub push JSON (it posts with content-type: application/json)
app.use(express.json({ type: "*/*" }));

// Extract IG comment events from the envelope you published from the webhook
function extractCommentEvents(envelope) {
  const events = [];
  const entries = envelope?.body?.entry || [];
  for (const entry of entries) {
    const changes = entry?.changes || [];
    for (const ch of changes) {
      const v = ch?.value || {};
      if (v.comment_id || v.text || v.media_id) {
        events.push({
          eventId: envelope?.headers?.["X-Hub-Delivery"] || v.id || v.comment_id,
          pageId: entry?.id,
          mediaId: v.media_id || v.media?.id,
          commentId: v.comment_id,
          text: v.text,
          fromUserId: v.from?.id,
          fromUsername: v.from?.username,
          timestamp: v.timestamp || v.time,
        });
      }
    }
  }
  return events;
}

// Pub/Sub push endpoint (this is what you'll set as the push URL)
app.post("/pubsub", async (req, res) => {
  const msg = req.body?.message;
  if (!msg || !msg.data) {
    console.warn("No Pub/Sub message in request:", JSON.stringify(req.body || {}));
    return res.status(204).send(); // ACK anyway so Pub/Sub doesn't retry forever
  }

  let envelope;
  try {
    const json = Buffer.from(msg.data, "base64").toString("utf8");
    envelope = JSON.parse(json);
  } catch (e) {
    console.error("Failed to decode/parse message.data:", e);
    return res.status(204).send();
  }

  console.log("Envelope:", JSON.stringify(envelope));

  const commentEvents = extractCommentEvents(envelope);
  if (commentEvents.length === 0) {
    console.log("No comment events detected.");
  } else {
    for (const c of commentEvents) {
      console.log("IG Comment Event:", {
        eventId: c.eventId,
        pageId: c.pageId,
        mediaId: c.mediaId,
        commentId: c.commentId,
        from: { id: c.fromUserId, username: c.fromUsername },
        text: c.text,
        timestamp: c.timestamp,
      });
    }
  }

  return res.status(204).send(); // ACK
});

// Health check
app.get("/", (_req, res) => res.status(200).send("ok"));

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Worker listening on ${PORT}`));
