import mongoose from "mongoose";
const { Schema } = mongoose;

const RepliedCommentSchema = new Schema(
  {
    commentId: { type: String, required: true, index: true },
    automationId: { type: Schema.Types.ObjectId, ref: "automations", required: true, index: true },
    channel: { type: String, enum: ["public", "private"], required: true }, // NEW
    status: { type: String, enum: ["sent", "failed"], default: "sent" },    // NEW
    repliedAt: { type: Date, default: Date.now },
    text: { type: String },
    error: { type: Object },
    type: { type: String }, // keep your existing if you use it elsewhere
  },
  { timestamps: true }
);

// One record per automation/comment/channel (not strictly required if ActionLock is used,
// but great for clean dashboards + queries)
RepliedCommentSchema.index({ automationId: 1, commentId: 1, channel: 1 }, { unique: true });

const RepliedComment =
  mongoose.models.RepliedComment ||
  mongoose.model("RepliedComment", RepliedCommentSchema, "replied_comments");

export default RepliedComment;
