// models/ActionLock.js
import mongoose from "mongoose";
const { Schema } = mongoose;


const ActionLockSchema = new Schema(
  {
    automationId: { type: Schema.Types.ObjectId, ref: "automations", required: true },
    commentId: { type: String, required: true },
    postId: { type: String, required: true },  // Add postId
  igUserId: { type: String, required: true },  // Add igUserId
  textHash: { type: String }, // Optional: hash of comment text
    channel: { type: String, enum: ["public", "private"], required: true },
    state: { type: String, enum: ["reserved", "sent", "failed"], default: "reserved" },
    reservedAt: { type: Date, default: Date.now },
    sentAt: { type: Date },
    error: { type: Object },
  },
  { timestamps: true }
);

// Unique constraint index
ActionLockSchema.index(
  { automationId: 1, postId:1, igUserId: 1, textHash: 1, channel: 1 },
  { unique: true }
);
// If you query by commentId alone frequently, keep this
ActionLockSchema.index({ automationId: 1 });

const ActionLock =
  mongoose.models.ActionLock ||
  mongoose.model("ActionLock", ActionLockSchema, "action_locks");

export default ActionLock;
