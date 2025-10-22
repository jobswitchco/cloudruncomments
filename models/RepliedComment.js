import mongoose from "mongoose";

const { Schema } = mongoose;

const RepliedCommentSchema = new Schema(
  {
    commentId: { type: String, required: true, unique: true, index: true },
    automationId: { type: Schema.Types.ObjectId, ref: "automations", required: true },
    repliedAt: { type: Date, default: Date.now },
    text: { type: String },
    type:{ type : String}
  },
  { timestamps: true }
);

const RepliedComment =
  mongoose.models.RepliedComment ||
  mongoose.model("RepliedComment", RepliedCommentSchema, "replied_comments");

export default RepliedComment;
