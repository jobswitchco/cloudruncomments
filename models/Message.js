// models/Message.js
import mongoose from "mongoose";
const { Schema } = mongoose;

const MessageSchema = new Schema(
  {
    conversationId: {
      type: Schema.Types.ObjectId,
      ref: "conversations",
      required: true,
    },

    platform: { type: String, enum: ["instagram"], required: true },
    igMessageId: { type: String, required: true },

    /* ---------- SENDER ---------- */
    sender: {
      type: String,
      enum: ["me", "them"],
      required: true,
    },

    // Message schema
action: {
  label: { type: String },
  url: { type: String }
},



    isRead: { type: Boolean, default: false },
    metaCursor: { type: String },

    senderType: {
      type: String,
      enum: ["creator", "participant"],
      required: true,
    },

    senderId: {
      type: Schema.Types.ObjectId,
      required: true,
      refPath: "senderTypeRef",
    },

    senderTypeRef: {
      type: String,
      required: true,
      enum: ["users", "participants"],
    },

    /* ---------- CONTENT ---------- */
    type: {
      type: String,
      enum: ["text", "image", "video", "reaction", "system"],
      default: "text",
    },

    text: { type: String },
    mediaUrl: { type: String },
    mediaType: { type: String, enum: ["image", "video"] },

    isDeleted: { type: Boolean, default: false },

    createdAtPlatform: { type: Date },
  },
  { timestamps: true }
);

/* ---------- INDEXES ---------- */

// Prevent duplicate messages
MessageSchema.index({ igMessageId: 1 }, { unique: true });

// Fast chat pagination
MessageSchema.index({ conversationId: 1, createdAtPlatform: -1 });

// Sender analytics
MessageSchema.index({ senderType: 1, senderId: 1 });

const Message =
  mongoose.models.Message ||
  mongoose.model("Message", MessageSchema, "messages");

export default Message;
