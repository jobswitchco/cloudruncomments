// services/conversationDiscovery.js
import axios from "axios";
import Conversation from "../models/Conversation.js";
import Participant from "../models/Participant.js";
import Message from "../models/Message.js";
import instagramService from "./instagramService.js";
import { publishConversationCreated } from "./realtimePublisher.js";

const GRAPH_API_BASE = "https://graph.facebook.com/v24.0";

/**
 * Find or create a conversation when we only know the participant's IG User ID
 * This happens when a message arrives for a conversation not yet in our DB
 */
export async function findOrCreateConversationByParticipant({
  creatorId,
  participantIgUserId,
  businessIgUserId,
  pageAccessToken,
  fbPageId,
}) {
  try {
    console.log("🔍 Searching for conversation with participant:", participantIgUserId);

    // ✅ ALWAYS use standardized igConversationId format
    const igConversationId = `igdm:${businessIgUserId}:${participantIgUserId}`;

    // STEP 1: Check if conversation already exists with this exact ID
    let conversation = await Conversation.findOne({
      creatorId: creatorId,
      platform: "instagram",
      igConversationId: igConversationId,
    });

    if (conversation) {
      console.log("✅ Found existing conversation:", conversation._id);
      
      const participant = await Participant.findById(conversation.participantId);
      
      return {
        conversation: conversation,
        participant: participant,
        isNew: false,
      };
    }

    // STEP 2: No conversation exists - need to fetch from Meta and create

    // STEP 2a: Find the Meta conversation ID (needed for fetching messages)
    console.log("📡 Finding conversation in Meta API...");
    const metaConversationId = await findConversationIdFromMeta({
      businessIgUserId,
      participantIgUserId,
      pageAccessToken,
      fbPageId,
    });

    if (!metaConversationId) {
      throw new Error("Could not find conversation ID from Meta");
    }

    console.log("✅ Found Meta conversation ID:", metaConversationId);

    // STEP 3: Fetch participant profile from Meta
    console.log("👤 Fetching participant profile...");
    const profileData = await instagramService.fetchUserProfile({
      igUserId: participantIgUserId,
      accessToken: pageAccessToken,
    });

    // STEP 4: Create or update participant
    const participant = await Participant.findOneAndUpdate(
      { platform: "instagram", igUserId: participantIgUserId },
      {
        $set: {
          username: profileData?.username || null,
          name: profileData?.name || null,
          profilePic: profileData?.profile_pic_url || null,
          lastSeenAt: new Date(),
        },
      },
      { upsert: true, new: true }
    );

    console.log("✅ Participant created/updated:", participant._id);

    // STEP 5: Fetch last 25 messages from Meta using the Meta conversation ID
    console.log("📥 Fetching last 25 messages...");
    const { messages: fetchedMessages, paging } =
      await instagramService.fetchLatestMessages({
        igConversationId: metaConversationId, // ✅ Use Meta ID for API calls
        accessToken: pageAccessToken,
        limit: 25,
      });

    console.log(`✅ Fetched ${fetchedMessages.length} messages`);

    // STEP 6: Create conversation record with standardized ID
    const lastMessage = buildLastMessageSnapshot(
      fetchedMessages[0],
      businessIgUserId
    );

// 🔥 NEW: Calculate unread count and lastParticipantMessageAt
let unreadCount = 0;
let lastParticipantMessageAt = null;

for (const msg of fetchedMessages) {
  const isFromBusiness = msg.from?.id === businessIgUserId;
  
  if (!isFromBusiness) {
    unreadCount++;
    
    // Track most recent message from participant
    const msgTime = new Date(msg.created_time);
    if (!lastParticipantMessageAt || msgTime > lastParticipantMessageAt) {
      lastParticipantMessageAt = msgTime;
    }
  }
}

    // ✅ Use standardized igConversationId format for DB lookups
    // ✅ Store Meta's conversation ID in metaThreadId for API calls
    conversation = await Conversation.create({
      platform: "instagram",
      igConversationId: igConversationId,     // igdm:17841402138259768:2226812364460274
      metaThreadId: metaConversationId,       // aWdfZAG06MTpJR01lc3NhZA2VU...
      creatorId: creatorId,
      participantId: participant._id,
      lastMessage: lastMessage,
      lastActivityAt: new Date(fetchedMessages[0]?.created_time || Date.now()),
      lastSyncedAt: new Date(),
      lastMetaCursor: paging?.cursors?.after || null,
      unreadCount: unreadCount,
      lastParticipantMessageAt: lastParticipantMessageAt,
      label: "General",
      labelSource: "auto",
    });

    console.log("✅ Conversation created:", conversation._id);
    await conversation.populate('participantId');

    // STEP 7: Save all fetched messages
    console.log("💾 Saving messages to database...");
    const savedMessages = await saveMessagesToDatabase({
      messages: fetchedMessages,
      conversationId: conversation._id,
      participantId: participant._id,
      creatorId: creatorId,
      businessIgUserId: businessIgUserId,
    });

    console.log(`✅ Saved ${savedMessages.length} messages`);

     // 🔥 NEW: Publish to Redis so frontend gets the new conversation
await publishConversationCreated({
  creatorId: creatorId,
  conversation: {
    ...conversation.toObject(),
    participant: participant.toObject ? participant.toObject() : participant,
    canReply: lastParticipantMessageAt 
      ? (Date.now() - new Date(lastParticipantMessageAt).getTime() <= 24 * 60 * 60 * 1000)
      : false,
    unreadCount: conversation.unreadCount, // ✅ Make sure this is included
  },
});

    console.log("✅ Published new conversation to Redis");

    return {
      conversation,
      participant,
      messages: savedMessages,
      isNew: true,
    };
  } catch (error) {
    console.error("❌ findOrCreateConversationByParticipant failed:", error.message);
    throw error;
  }
}

/**
 * Find the Instagram Conversation ID by searching through the creator's conversations
 * Returns the Meta conversation ID (needed for API calls to fetch messages)
 */
async function findConversationIdFromMeta({
  businessIgUserId,
  participantIgUserId,
  pageAccessToken,
  fbPageId,
}) {
  try {
    let after = null;
    let attempts = 0;
    const maxAttempts = 5; // Check up to 50 conversations (5 pages × 10)

    while (attempts < maxAttempts) {
      const { data, paging } = await instagramService.fetchConversations({
        pageId: fbPageId, // ✅ Use FB Page ID
        accessToken: pageAccessToken,
        limit: 10,
        after: after,
      });

      // Search through this page of conversations
      for (const conv of data) {
        const participants = conv.participants?.data || [];
        
        // Check if this conversation includes our target participant
        const hasParticipant = participants.some(
          (p) => p.id === participantIgUserId
        );

        if (hasParticipant) {
          console.log("✅ Found Meta conversation ID:", conv.id);
          return conv.id; // Return the Meta conversation ID
        }
      }

      // Check if there are more pages
      if (!paging?.next) {
        break;
      }

      after = paging.cursors?.after;
      attempts++;
    }

    console.warn("⚠️ Conversation not found in first 50 conversations");
    return null;
  } catch (error) {
    console.error("❌ findConversationIdFromMeta error:", error.message);
    throw error;
  }
}

/**
 * Build last message snapshot for conversation
 */
function buildLastMessageSnapshot(message, businessIgUserId) {
  if (!message) {
    return {
      text: "No messages yet",
      type: "system",
      sender: "them",
      timestamp: new Date(),
    };
  }

  const isFromBusiness = message.from?.id === businessIgUserId;
  let type = "text";
  let text = message.message || "";

  // Handle attachments
  if (message.attachments && message.attachments.length > 0) {
    const attachment = message.attachments[0];
    if (attachment.image_data) {
      type = "image";
      text = "Sent an image";
    } else if (attachment.video_data) {
      type = "video";
      text = "Sent a video";
    }
  }

  // Handle unsupported content
  if (message.is_unsupported) {
    type = "system";
    text = "Shared unsupported content";
  }

  return {
    text: text || "Sent an attachment",
    type: type,
    sender: isFromBusiness ? "me" : "them",
    timestamp: new Date(message.created_time),
  };
}

/**
 * Save messages to database in bulk
 */
async function saveMessagesToDatabase({
  messages,
  conversationId,
  participantId,
  creatorId,
  businessIgUserId,
}) {
  try {
    const messageDocs = messages.map((msg) => {
      const isFromBusiness = msg.from?.id === businessIgUserId;
      
      let type = "text";
      let text = msg.message || null;
      let mediaUrl = null;
      let mediaType = null;
      let action = null;

      // Handle attachments
      if (msg.attachments && msg.attachments.length > 0) {
        const attachment = msg.attachments[0];
        
        if (attachment.image_data) {
          type = "image";
          mediaType = "image";
          mediaUrl = attachment.image_data?.url || attachment.file_url;
        } else if (attachment.video_data) {
          type = "video";
          mediaType = "video";
          mediaUrl = attachment.video_data?.url || attachment.file_url;
        }
      }

      // Handle unsupported content
      if (msg.is_unsupported) {
        type = "system";
        text = "Shared unsupported content";
        action = {
          label: "View on Instagram",
          url: "https://www.instagram.com/direct/inbox/",
        };
      }

      return {
        conversationId: conversationId,
        platform: "instagram",
        igMessageId: msg.id,
        sender: isFromBusiness ? "me" : "them",
        senderType: isFromBusiness ? "creator" : "participant",
        senderId: isFromBusiness ? creatorId : participantId,
        senderTypeRef: isFromBusiness ? "users" : "participants",
        type: type,
        text: text,
        mediaUrl: mediaUrl,
        mediaType: mediaType,
        action: action,
        isRead: isFromBusiness, // Creator's own messages are always read
        createdAtPlatform: new Date(msg.created_time),
      };
    });

    // Insert messages, ignoring duplicates
    const result = await Message.insertMany(messageDocs, { ordered: false });
    return result;
  } catch (error) {
    // If error is duplicate key, some messages were already saved - that's OK
    if (error.code === 11000) {
      console.log("ℹ️ Some messages already exist (duplicate key), continuing...");
      return [];
    }
    throw error;
  }
}