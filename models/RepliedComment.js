import mongoose from "mongoose";
const { Schema } = mongoose;

const RepliedCommentSchema = new Schema(
  {
    commentId: { type: String, required: true, index:true },
    automationId: { type: Schema.Types.ObjectId, ref: "automations", required: true, index:true },
    channel: { type: String, enum: ["public", "private"], required: true, index:true }, // NEW
    status: { type: String, enum: ["sent", "failed"], default: "sent" },    // NEW
    repliedAt: { type: Date, default: Date.now },
    text: { type: String },
    sentMessage: { type: String },
    error: { type: Object },
    type: { type: String }, // keep your existing if you use it elsewhere
    igUserId: String, // IGSID
    username: String,
    profilePic: String,
    followsBusiness: Boolean,
    businessFollowsUser: Boolean,
  },
  { timestamps: true }
);

// One record per automation/comment/channel (not strictly required if ActionLock is used,
// but great for clean dashboards + queries)
RepliedCommentSchema.index({ automationId: 1, commentId: 1, channel: 1 });

const RepliedComment =
  mongoose.models.RepliedComment ||
  mongoose.model("RepliedComment", RepliedCommentSchema, "replied_comments");

export default RepliedComment;
