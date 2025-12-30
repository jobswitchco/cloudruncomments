// models/ActionLock.js
import mongoose from "mongoose";
const { Schema } = mongoose;

const ActionLockSchema = new Schema(
  {
    automationId: {
      type: Schema.Types.ObjectId,
      ref: "automations",
      required: true,
      index: true,
    },

    // Context
    commentId: { type: String, required: true },
    postId: { type: String },
    postType: { type: String },

    igUserId: { type: String, required: true },

    // Used for dedupe across similar comments
    textHash: { type: String },

    channel: {
      type: String,
      enum: ["public", "private"],
      required: true,
      index: true,
    },

    // Lifecycle
    state: {
      type: String,
      enum: ["reserved", "queued", "sent", "failed"],
      default: "reserved",
      index: true,
    },

    reservedAt: { type: Date, default: Date.now },
    scheduledAt: { type: Date, index: true },
    sentAt: { type: Date },

    // Retry & diagnostics
    attempt: { type: Number, default: 0 },
    lastError: { type: Object },

    /**
     * EVERYTHING needed to execute the action later
     * Agenda jobs must rely ONLY on this
     */
    payload: {
      // Public reply
      replyText: String,

      // Private DM
      dmMessage: String,
      buttonText: String,

      // Execution context
      pageId: String,
      creatorId: {
        type: Schema.Types.ObjectId,
        ref: "users",
      },
      automationId: {
        type: Schema.Types.ObjectId,
        ref: "automations",
      },
      igUserId: String,
      commentId: String,
    },
  },
  { timestamps: true }
);

/**
 * 🔒 STRONG IDEMPOTENCY
 * Prevents duplicate public/private actions
 */
ActionLockSchema.index(
  {
    automationId: 1,
    postId: 1,
    commentId: 1,
    igUserId: 1,
    channel: 1,
  },
  { unique: true }
);

/**
 * ⏱ Used by Agenda poller
 */
ActionLockSchema.index({
  state: 1,
  scheduledAt: 1,
});

const ActionLock =
  mongoose.models.ActionLock ||
  mongoose.model("ActionLock", ActionLockSchema, "action_locks");

export default ActionLock;
