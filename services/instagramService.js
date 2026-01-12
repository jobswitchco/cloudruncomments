//instagramService.js
import axios from 'axios';

const GRAPH_API_VERSION = 'v24.0';
const GRAPH_API_BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;

class InstagramService {


// services/instagramService.js

async fetchOlderMessages({
  igConversationId,
  pageAccessToken,
  afterCursor,
  limit = 25,
}) {
  try {
    const url = `${GRAPH_API_BASE}/${igConversationId}/messages`;

    const params = {
      access_token: pageAccessToken,
      limit,
      fields:
        "id,created_time,is_unsupported,from,to,message,attachments{mime_type,file_url,image_data,video_data}",
    };

    if (afterCursor) {
      params.after = afterCursor;
    }

    const res = await axios.get(url, { params });

    return {
      messages: res.data?.data || [],
      paging: res.data?.paging || {},
    };
  } catch (err) {
    console.error(
      "Fetch older IG messages failed:",
      err.response?.data || err
    );
    throw err;
  }
}

 async fetchUserProfile({ igUserId, accessToken }) {
    try {
      const url = `https://graph.facebook.com/v24.0/${igUserId}`;
      
      const response = await axios.get(url, {
        params: {
          fields: "id,username,name,profile_pic,follower_count,is_user_follow_business,is_business_follow_user",
          access_token: accessToken,
        },
        timeout: 10000, // 10 second timeout
      });

      if (!response.data) {
        console.warn(`⚠️ No profile data for ${igUserId}`);
        return null;
      }

      // Return normalized profile
      return {
        id: response.data.id,
        username: response.data.username || null,
        name: response.data.name || null,
        profile_pic_url: response.data.profile_pic || null,
        follower_count: response.data.follower_count || 0,
        is_following_business: response.data.is_user_follow_business || false,
        is_followed_by_business: response.data.is_business_follow_user || false,
        is_private: false, // Instagram Graph API doesn't expose this directly
      };
    } catch (err) {
      // Handle specific errors
      if (err.response?.status === 400) {
        console.error(`❌ Invalid Instagram User ID: ${igUserId}`);
      } else if (err.response?.status === 403) {
        console.error(`❌ Access denied for user ${igUserId} - may be restricted`);
      } else if (err.code === "ECONNABORTED") {
        console.error(`❌ Timeout fetching profile for ${igUserId}`);
      } else {
        console.error(`❌ fetchUserProfile failed for ${igUserId}:`, err.message);
      }
      
      return null;
    }
  }


 async sendMessage({ pageId, accessToken, payload }) {
    try {
      const res = await axios.post(
        `https://graph.facebook.com/v24.0/${pageId}/messages`,
        payload,
        {
          params: {
            access_token: accessToken,
          },
        }
      );

      return res.data;
    } catch (error) {
      console.error(
        "Instagram send message error:",
        error.response?.data || error
      );
      throw error;
    }
  }

async fetchConversations({
  pageId,
  accessToken,
  limit = 10,
  after = null,
}) {
  const url = `${GRAPH_API_BASE}/${pageId}/conversations`;

  const params = {
    access_token: accessToken,
    platform: "instagram",
    limit,
    fields: "id,participants",
  };

  // 🔑 Cursor support
  if (after) {
    params.after = after;
  }

  const res = await axios.get(url, { params });

  return {
    data: res.data?.data || [],
    paging: res.data?.paging || null,
  };
}


async fetchLatestMessages({
  igConversationId,
  accessToken,
  afterCursor = null,
  limit = 25,
}) {
  try {
    const params = {
      access_token: accessToken,
      limit,
      fields:
        "id,created_time,is_unsupported,from,to,message,attachments{mime_type,file_url,image_data,video_data}",
    };

    // 🔥 USE AFTER CURSOR FOR LATEST SYNC
    if (afterCursor) {
      params.after = afterCursor;
    }

    const res = await axios.get(
      `${GRAPH_API_BASE}/${igConversationId}/messages`,
      { params }
    );

    return {
      messages: res.data?.data || [],
      paging: res.data?.paging || {},
    };
  } catch (err) {
    console.error(
      "Fetch latest IG messages failed:",
      err.response?.data || err
    );
    throw err;
  }
}






}

export default new InstagramService();
