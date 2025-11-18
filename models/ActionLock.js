// models/ActionLock.js
import mongoose from "mongoose";
const { Schema } = mongoose;


const ActionLockSchema = new Schema(
  {
    automationId: { type: Schema.Types.ObjectId, ref: "automations", required: true },
    commentId: { type: String, required: true },
    channel: { type: String, enum: ["public", "private"], required: true },
    state: { type: String, enum: ["reserved", "sent", "failed"], default: "reserved" },
    reservedAt: { type: Date, default: Date.now },
    sentAt: { type: Date },
    error: { type: Object },
  },
  { timestamps: true }
);

// Unique constraint index
ActionLockSchema.index({ automationId: 1, commentId: 1, channel: 1 }, { unique: true });


// Query optimization index for (automationId, channel, state) queries
ActionLockSchema.index({ automationId: 1, channel: 1, state: 1 });

// If you query by commentId alone frequently, keep this
ActionLockSchema.index({ automationId: 1 });

const ActionLock =
  mongoose.models.ActionLock ||
  mongoose.model("ActionLock", ActionLockSchema, "action_locks");

export default ActionLock;
