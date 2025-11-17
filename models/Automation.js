import mongoose from "mongoose";
const { Schema } = mongoose;

const AutomationSchema = new Schema(
  {
    userId: { type: Schema.Types.ObjectId, ref: "users", required: true },
    platform: { type: String, enum: ["instagram"], default: "instagram" },
    postId: { type: String, required: true },
    repliedCount: { type: Number, default: 0 },
    thumbnail: { type: String },
    postLive: { type: Boolean, default: true },
    lastCheckedAt: { type: Date, default: Date.now },
    caption: { type: String, default: null, trim: true },
    dmMessage: { type: String},
    buttonText: { type: String},
    flowNodes: { type: Array, default: []},
    keywords: { type: Array, default: []},
    hasReply: { type: Boolean, default: false},
    replyComment: { type: String},

    createdAt: { type: Date },
    status: {
      type: String,
      enum: ["active", "paused", "archived", "inactive"],
      default: "active",
      index: true,
    },

  },
  { timestamps: true }
);

AutomationSchema.index({ userId: 1, postId: 1 });
AutomationSchema.index({ platform: 1, postId: 1, status: 1 });
AutomationSchema.index({ postLive: 1, userId: 1 });

const Automation =
  mongoose.models.Automation ||
  mongoose.model("Automation", AutomationSchema, "automations");
export default Automation;
