import mongoose from "mongoose";
const { Schema } = mongoose;

const RepliedCommentSchema = new Schema(
  {
    commentId: { type: String, required: true },
    automationId: { type: Schema.Types.ObjectId, ref: "automations", required: true },
    postId: { type: String, required: true }, // linked Instagram post ID
    userId: { type: Schema.Types.ObjectId, ref: "users", required: true },

    // Track which node in flow this comment reply is on
    currentNodeId: { type: String },

    repliedAt: { type: Date, default: Date.now },
    text: { type: String },            // comment's text content
    sentMessage: { type: String },    // message sent as reply or DM
    error: { type: String },           // error message if any during reply flow

    type: { type: String },            // optional usage if needed

    igUserId: String,
    username: String,
    profilePic: String,
    followsBusiness: Boolean,
    businessFollowsUser: Boolean,

    status: {
      type: String,
      enum: ["pending", "replied", "completed", "failed", "active"],
      default: "pending",
    },
  },
  { timestamps: true }
);

// Updated index - removed 'channel' as it's not in schema
RepliedCommentSchema.index({ automationId: 1, commentId: 1 });
RepliedCommentSchema.index(
  { postId: 1, igUserId: 1, text: 1 },
  { unique: true, partialFilterExpression: { status: { $ne: "failed" } } }
);


const RepliedComment =
  mongoose.models.RepliedComment ||
  mongoose.model("RepliedComment", RepliedCommentSchema, "replied_comments");

export default RepliedComment;
