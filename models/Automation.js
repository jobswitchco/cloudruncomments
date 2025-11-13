// models/Automation.js
import mongoose from "mongoose";
const { Schema } = mongoose;

// Simple button (for backward compatibility)
const ButtonSchema = new Schema(
  {
    text: { type: String, trim: true },
    url: { type: String, trim: true },
  },
  { _id: false }
);

// Flow node structure (used in conversation flows)
const FlowNodeSchema = new Schema(
  {
    message: { type: String, trim: true, required: true },
    type: {
      type: String,
      enum: ["quick_replies", "button", "generic", "media", "text"],
      default: "text",
    },
    // Quick replies (up to 13)
    quick_replies: [
      {
        title: { type: String, trim: true },
        payload: { type: String, trim: true },
      },
    ],
    // Buttons (up to 3)
    buttons: [
      {
        text: { type: String, trim: true },
        url: { type: String, trim: true },
      },
    ],
    // Generic/Carousel cards (up to 10)
    cards: [
      {
        title: { type: String, trim: true },
        subtitle: { type: String, trim: true },
        image_url: { type: String, trim: true },
        button: {
          text: { type: String, trim: true },
          url: { type: String, trim: true },
        },
      },
    ],
    // Media attachment
    media_url: { type: String, trim: true },
    
    // Next actions: maps payload (from quick_replies) → next flow ID
    next_actions: {
      type: Map,
      of: String,
      default: () => new Map(),
    },
  },
  { _id: false }
);

// Flow configuration (entire conversation tree)
const FlowConfigSchema = new Schema(
  {
    initial: { type: FlowNodeSchema, required: true },
    flows: {
      type: Map,
      of: FlowNodeSchema,
      default: () => new Map(),
    },
  },
  { _id: false }
);

// DM Schema (supports both simple and flow-based DMs)
const DMSchema = new Schema(
  {
    enabled: { type: Boolean, default: false },
    
    // Type: "simple" for backward compatibility, "conversation_flow" for new flows
    type: {
      type: String,
      enum: ["simple", "conversation_flow"],
      default: "simple",
    },
    
    // For simple DM (backward compatible)
    message: { type: String, trim: true },
    button: { type: ButtonSchema, default: undefined },
    
    // For conversation flows (NEW)
    flowConfig: { type: FlowConfigSchema, default: undefined },
  },
  { _id: false }
);

const MediaSchema = new Schema(
  {
    thumbnail: { type: String, trim: true },
    caption: { type: String, trim: true },
  },
  { _id: false }
);

const RunStatsSchema = new Schema(
  {
    repliesSent: { type: Number, default: 0 },
    dmsSent: { type: Number, default: 0 },
    flowConversationsStarted: { type: Number, default: 0 }, // NEW
    flowConversationsCompleted: { type: Number, default: 0 }, // NEW
    lastRunAt: { type: Date },
  },
  { _id: false }
);

const AutomationSchema = new Schema(
  {
    userId: { type: Schema.Types.ObjectId, ref: "users", required: true },
    platform: { type: String, enum: ["instagram"], default: "instagram" },
    postId: { type: String, required: true },
    repliedCount: { type: Number, default: 0 },
    thumbnail: { type: String },
    postLive: { type: Boolean, default: true },
    lastCheckedAt: { type: Date, default: Date.now },
    
    keywords: {
      type: [String],
      default: [],
      validate: {
        validator: (arr) => Array.isArray(arr) && arr.length > 0,
        message: "At least one keyword is required",
      },
      set: (arr) =>
        [...new Set(arr.map((k) => String(k || "").trim()).filter(Boolean))],
    },
    
    publicReply: { type: String, default: null, trim: true },
    caption: { type: String, default: null, trim: true },
    hasPublicReply: { type: Boolean, default: false },
    
    dm: { type: DMSchema, default: { enabled: false } },
    
    createdAt: { type: Date },
    media: { type: MediaSchema, default: undefined },
    
    status: {
      type: String,
      enum: ["active", "paused", "archived", "inactive"],
      default: "active",
      index: true,
    },
    
    runStats: { type: RunStatsSchema, default: () => ({}) },
  },
  { timestamps: true }
);

// Indexes
AutomationSchema.index({ userId: 1, postId: 1 }, { unique: true });
AutomationSchema.index({ platform: 1, postId: 1, status: 1 });
AutomationSchema.index({ "runStats.lastRunAt": 1 });
AutomationSchema.index({ postLive: 1, userId: 1 });

const Automation =
  mongoose.models.Automation ||
  mongoose.model("Automation", AutomationSchema, "automations");

export default Automation;
