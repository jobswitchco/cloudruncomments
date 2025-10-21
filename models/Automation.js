// models/Automation.js
import mongoose from "mongoose";
const { Schema } = mongoose;


const ButtonSchema = new Schema(
  {
    text: { type: String, trim: true },
    url: { type: String, trim: true },
  },
  { _id: false }
);

const DMSchema = new Schema(
  {
    enabled: { type: Boolean, default: false, index: true },
    message: { type: String, trim: true },
    button: { type: ButtonSchema, default: undefined },
  },
  { _id: false }
);

const MediaSchema = new Schema(
  {
    // Optional: store a snapshot of media for context
    thumbnail: { type: String, trim: true },
    caption: { type: String, trim: true },
  },
  { _id: false }
);

const RunStatsSchema = new Schema(
  {
    repliesSent: { type: Number, default: 0 },
    dmsSent: { type: Number, default: 0 },
    lastRunAt: { type: Date },
  },
  { _id: false }
);

const AutomationSchema = new Schema(
  {
    userId: { type: Schema.Types.ObjectId, ref: "users", required: true, index: true },
    platform: { type: String, enum: ["instagram"], default: "instagram", index: true },

    postId: { type: String, required: true, index: true }, // IG media id
    repliedCount: { type: Number, default : 0 },
    thumbnail: { type: String},
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

    // Step 2: optional public reply
    publicReply: { type: String, default: null, trim: true },
    caption: { type: String, default: null, trim: true },
    hasPublicReply: { type: Boolean, default: false },

    // Step 3: DM config
    dm: { type: DMSchema, default: { enabled: false } },
    createdAt: { type: Date},

    // Optional media snapshot
    media: { type: MediaSchema, default: undefined },

    status: {
      type: String,
      enum: ["active", "paused", "archived"],
      default: "active",
      index: true,
    },

    // Runtime stats (optional, for dashboarding)
    runStats: { type: RunStatsSchema, default: () => ({}) },
  },
  { timestamps: true }
);

// Ensure one automation per user per post
// AutomationSchema.index({ userId: 1, postId: 1 }, { unique: true });

const Automation = mongoose.models.Automation || mongoose.model("Automation", AutomationSchema, "automations");
export default Automation;