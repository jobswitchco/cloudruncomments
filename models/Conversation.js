// models/Conversation.js
import mongoose from "mongoose";
const { Schema } = mongoose;

const LastMessageSchema = new Schema(
  {
    text: { type: String },
    type: { type: String, enum: ["text", "image", "video", "system"], default: "text" },
    sender: { type: String, enum: ["me", "them"], required: true },
    timestamp: { type: Date, required: true },
  },
  { _id: false }
);

const ConversationSchema = new Schema(
  {
    // Platform info
    platform: { type: String, enum: ["instagram"], required: true },
    igConversationId: { type: String, required: true },
    metaThreadId: { type: String },

    // Owner (creator)
    creatorId: { type: Schema.Types.ObjectId, ref: "users", required: true },

    // Other participant
    participantId: { type: Schema.Types.ObjectId, ref: "Participant", required: true },

    // Labeling
    label: {
      type: String,
      enum: ["Personal", "Lead", "General"],
      default: "General",
    },
    labelSource: {
      type: String,
      enum: ["auto", "manual"],
      default: "auto",
    },

    // State
    isBlocked: { type: Boolean, default: false },

    // Inbox snapshot
    lastMessage: { type: LastMessageSchema },
    unreadCount: { type: Number, default: 0 },
    lastSyncedAt: Date,
    lastMetaCursor: String,
    lastParticipantMessageAt: {
  type: Date,
  index: true
},


    // Sorting
    lastActivityAt: { type: Date, required: true },
  },
  { timestamps: true }
);

/* ---------- INDEXES ---------- */

// Prevent duplicate conversations per creator
ConversationSchema.index(
  { creatorId: 1, igConversationId: 1 },
  { unique: true }
);

// Inbox loading & sorting
ConversationSchema.index(
  { creatorId: 1, lastActivityAt: -1 }
);

// Label-based filtering
ConversationSchema.index(
  { creatorId: 1, label: 1 }
);

// Participant lookup
ConversationSchema.index(
  { participantId: 1 }
);

ConversationSchema.index(
  { creatorId: 1, platform: 1, igConversationId: 1 },
  { unique: true }
);


const Conversation =
  mongoose.models.Conversation ||
  mongoose.model("Conversation", ConversationSchema, "conversations");

export default Conversation;
