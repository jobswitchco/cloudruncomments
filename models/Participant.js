// models/Participant.js
import mongoose from "mongoose";
const { Schema } = mongoose;

const ParticipantSchema = new Schema(
  {
    platform: { type: String, enum: ["instagram"], required: true },

    igUserId: { type: String, required: true },

    username: { type: String },
    name: { type: String },
    profilePic: { type: String },

    lastSeenAt: { type: Date },
  },
  { timestamps: true }
);

/* ---------- INDEXES ---------- */

// One participant per platform user
ParticipantSchema.index(
  { platform: 1, igUserId: 1 },
  { unique: true }
);

// Lookup by username (optional but useful)
ParticipantSchema.index(
  { username: 1 }
);

const Participant =
  mongoose.models.Participant ||
  mongoose.model("Participant", ParticipantSchema, "participants");

export default Participant;
