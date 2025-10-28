// models/ActionLock.js
import mongoose from "mongoose";
const { Schema } = mongoose;

/**
 * One document per unique action you might perform:
 *   - channel: "public" | "private"
 *   - commentId + automationId + channel must be unique
 * States:
 *   - "reserved": we won the right to send, about to call Meta API
 *   - "sent": sent successfully
 *   - "failed": call failed; we store error (manual retry can clear state)
 */
const ActionLockSchema = new Schema(
  {
    automationId: { type: Schema.Types.ObjectId, ref: "automations", required: true, index: true },
    commentId: { type: String, required: true, index: true },
    channel: { type: String, enum: ["public", "private"], required: true },
    state: { type: String, enum: ["reserved", "sent", "failed"], default: "reserved", index: true },
    reservedAt: { type: Date, default: Date.now },
    sentAt: { type: Date },
    error: { type: Object },
  },
  { timestamps: true }
);

// Unique gate: never perform the same (automation, comment, channel) twice
ActionLockSchema.index({ automationId: 1, commentId: 1, channel: 1 });

const ActionLock =
  mongoose.models.ActionLock ||
  mongoose.model("ActionLock", ActionLockSchema, "action_locks");

export default ActionLock;
