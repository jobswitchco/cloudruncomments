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
    enabled: { type: Boolean, default: false },
    message: { type: String, trim: true },
    button: { type: ButtonSchema, default: undefined },
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
    postLive: { type: Boolean, default: true }, // NEW FIELD
    lastCheckedAt: { type: Date, default: Date.now }, // NEW FIELD - tracks last verification
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

AutomationSchema.index({ userId: 1, postId: 1 }, { unique: true });
AutomationSchema.index({ userId: 1}, { unique: true });
AutomationSchema.index({ platform: 1, postId: 1, status: 1 });
AutomationSchema.index({ "runStats.lastRunAt": 1 });
AutomationSchema.index({ postLive: 1, userId: 1 }); // NEW INDEX

const Automation = mongoose.models.Automation || mongoose.model("Automation", AutomationSchema, "automations");
export default Automation;
