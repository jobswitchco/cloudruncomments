const mongoose = require("mongoose");

const { Schema } = mongoose;

const User_Schema = new Schema({
  email: { type: String, required: true },
  name: { type: String },
  intro: { type: String },
  sub: { type: String },
  picture: { type: String },
  leftHeadImage: { type: String },
  rightTopImage: { type: String },
  rightBottomImage: { type: String },
  is_google_user: { type: Boolean },
  handleUserName: { type: String },
  
  instagramConnected: { type: Boolean, default: false },
  fbPageId: { type: String, index: true, required: true },
  igUserId: { type: String },
  igId: { type: String },
  igName: { type: String },
  igUsername: { type: String },
  igProfilePic: { type: String },
  igFollowersCount: { type: Number },
  igFollowsCount: { type: Number },
  igMediaCount: { type: Number },
  fbLongLivedToken: { type: String },
  fbTokenExpiry: { type: Date },
  igBiography: {type : String},
  fbPageAccessToken: {type : String},
  has_profile_pic_ig: { type: Boolean, default: false },
  primary: { type: Boolean, default: false }, 


  demo_logged_in: { type: Boolean },
  demo_logged_date: { type: Date },



  socials: [{
    platform: { type: String },
    url: { type: String },
    created_at: { type: Date, default: Date.now },
  }],
  account_delete_code: { type: Number },
  last_login: { type: Date },
  loginHistory: [{ type: Date }],
  free_trial: { type: Boolean, default: true },
  store_enabled: { type: Boolean, default: false },
  dm_enabled: { type: Boolean, default: false },
  free_trial_started_date: { type: Date, default: Date.now },
  is_del: { type: Boolean, default: false },
  created_at: { type: Date, default: Date.now },
  updated_at: { type: Date },
});

// Register model as "User" but use existing collection "users"
const User = mongoose.models.User || mongoose.model("User", User_Schema, "users");
export default User;
