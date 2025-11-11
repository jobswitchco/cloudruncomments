import mongoose from "mongoose";
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
  igUserId: { type: String },
  igId: { type: String },
  igName: { type: String },
  igUsername: { type: String },
  fbPageId: { type: String },
  igProfilePic: { type: String },
  igFollowersCount: { type: Number },
  igFollowsCount: { type: Number },
  igMediaCount: { type: Number },
  fbLongLivedToken: { type: String },
  fbLongLivedTokenExpiry: { type: Date },
  fbLastRefreshAt: { type: Date },
  igBiography: {type : String},
  fbPageAccessToken: {type : String},
  has_profile_pic_ig: { type: Boolean, default: false },
  fbNeedsReconnect: { type: Boolean },

  automationFeedSubscribed : {type : Boolean, default : false },


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
  free_trial_started_date: { type: Date, default: Date.now },
  free_trial_used: { type: Boolean, default: false },
  paid_subscription_active : { type: Boolean, default: false },

  store_enabled: { type: Boolean, default: false },
  dm_enabled: { type: Boolean, default: false },
  is_del: { type: Boolean, default: false },
  created_at: { type: Date, default: Date.now },
  updated_at: { type: Date },
});

User_Schema.index({ email: 1, handleUserName: 1 }, { unique: true });



// Register model as "User" but use existing collection "users"
const User = mongoose.models.User || mongoose.model("User", User_Schema, "users");
export default User;
