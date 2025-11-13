// models/ConversationState.js
import mongoose from "mongoose";
const { Schema } = mongoose;

// Conversation history entry
const ConversationHistorySchema = new Schema(
  {
    flowId: { type: String, required: true }, // "initial" or flow ID
    flowName: { type: String }, // e.g., "YES_STEP1", "NO_STEP1" (for display)
    messageSent: { type: String }, // Message sent to user
    userReply: { type: String }, // User's quick reply selection
    userPayload: { type: String }, // Payload from quick reply
    timestamp: { type: Date, default: Date.now },
  },
  { _id: false }
);

const ConversationStateSchema = new Schema(
  {
    // User references
    userId: {
      type: Schema.Types.ObjectId,
      ref: "users",
      required: true,
      index: true,
    },
    automationId: {
      type: Schema.Types.ObjectId,
      ref: "automations",
      required: true,
      index: true,
    },
    
    // Instagram identifiers
    commentId: { type: String, required: true, index: true },
    igUserId: { type: String, required: true, index: true }, // Instagram Scoped User ID
    igUsername: { type: String }, // Username for reference
    
    // Flow navigation state
    currentFlowId: {
      type: String,
      required: true,
      default: "initial",
    }, // "initial" or a key from flowConfig.flows
    
    // Store entire flow config for this conversation
    // (prevents issues if automation is edited mid-conversation)
    flowConfig: {
      type: Object,
      required: true,
    },
    
    // Conversation history (breadcrumb trail)
    conversationHistory: {
      type: [ConversationHistorySchema],
      default: [],
    },
    
    // Conversation metadata
    status: {
      type: String,
      enum: ["active", "completed", "expired", "error", "abandoned"],
      default: "active",
      index: true,
    },
    
    errorDetails: { type: Object }, // Store error if status = "error"
    
    // Timing
    startedAt: { type: Date, default: Date.now },
    lastInteractionAt: { type: Date, default: Date.now, index: true },
    completedAt: { type: Date },
    
    // Instagram messaging window (24 hours from comment)
    expiresAt: {
      type: Date,
      required: true,
      index: true,
    },
    
    // Analytics
    totalSteps: { type: Number, default: 0 }, // Number of flows user went through
    timeToComplete: { type: Number }, // Milliseconds (if completed)
  },
  { timestamps: true }
);

// Indexes for common queries
ConversationStateSchema.index({ igUserId: 1, status: 1 });
ConversationStateSchema.index({ automationId: 1, status: 1 });
ConversationStateSchema.index({ userId: 1, status: 1 });
ConversationStateSchema.index({ status: 1, lastInteractionAt: 1 });

// TTL index: auto-delete conversations 48 hours after expiry
ConversationStateSchema.index(
  { expiresAt: 1 },
  { expireAfterSeconds: 48 * 60 * 60 } // 48 hours
);

// Compound index for active conversation lookup
ConversationStateSchema.index({
  igUserId: 1,
  automationId: 1,
  status: 1,
});

// Methods
ConversationStateSchema.methods.addHistory = function (entry) {
  this.conversationHistory.push(entry);
  this.totalSteps = this.conversationHistory.length;
  this.lastInteractionAt = new Date();
};

ConversationStateSchema.methods.markCompleted = function () {
  this.status = "completed";
  this.completedAt = new Date();
  this.timeToComplete = this.completedAt - this.startedAt;
};

ConversationStateSchema.methods.markExpired = function () {
  this.status = "expired";
  this.completedAt = new Date();
};

ConversationStateSchema.methods.markError = function (error) {
  this.status = "error";
  this.errorDetails = {
    message: error.message,
    stack: error.stack,
    timestamp: new Date(),
  };
};

// Static methods
ConversationStateSchema.statics.findActiveConversation = async function (
  igUserId,
  automationId
) {
  return this.findOne({
    igUserId,
    automationId,
    status: "active",
    expiresAt: { $gt: new Date() },
  }).sort({ startedAt: -1 });
};

ConversationStateSchema.statics.expireOldConversations = async function () {
  const now = new Date();
  const result = await this.updateMany(
    {
      status: "active",
      expiresAt: { $lt: now },
    },
    {
      $set: {
        status: "expired",
        completedAt: now,
      },
    }
  );
  return result.modifiedCount;
};

const ConversationState =
  mongoose.models.ConversationState ||
  mongoose.model(
    "ConversationState",
    ConversationStateSchema,
    "conversation_states"
  );

export default ConversationState;
