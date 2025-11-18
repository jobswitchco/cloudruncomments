// models/ActionLock.js
import mongoose from "mongoose";
const { Schema } = mongoose;


const ProcessedEventSchema = new Schema(
  {
    eventId: { type: String, required: true, unique: true },
  processedAt: { type: Date, default: Date.now, expires: 604800 }
  },
);


// If you query by commentId alone frequently, keep this
ProcessedEventSchema.index({ eventId: 1 });

const ProcessedEvent =
  mongoose.models.ActionLock ||
  mongoose.model("ProcessedEvent", ProcessedEventSchema, "processed_event");

export default ProcessedEvent;
