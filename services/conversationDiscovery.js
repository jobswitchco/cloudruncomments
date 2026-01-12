// services/conversationDiscovery.js
import axios from "axios";
import Conversation from "../models/Conversation.js";
import Participant from "../models/Participant.js";
import Message from "../models/Message.js";
import instagramService from "./instagramService.js";

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
}) {
  try {
    console.log("🔍 Searching for conversation with participant:", participantIgUserId);

    // STEP 1: Check if participant already has a conversation with this creator
    const existingParticipant = await Participant.findOne({
      platform: "instagram",
      igUserId: participantIgUserId,
    });

    if (existingParticipant) {
      const existingConversation = await Conversation.findOne({
        creatorId: creatorId,
        participantId: existingParticipant._id,
      });

      if (existingConversation) {
        console.log("✅ Found existing conversation:", existingConversation.igConversationId);
        return {
          conversation: existingConversation,
          participant: existingParticipant,
          isNew: false,
        };
      }
    }

    // STEP 2: Fetch conversation from Meta API
    console.log("📡 Fetching conversation from Meta API...");
    const igConversationId = await findConversationIdFromMeta({
      businessIgUserId,
      participantIgUserId,
      pageAccessToken,
    });

    if (!igConversationId) {
      throw new Error("Could not find conversation ID from Meta");
    }

    console.log("✅ Found igConversationId:", igConversationId);

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

    // STEP 5: Fetch last 25 messages from Meta
    console.log("📥 Fetching last 25 messages...");
    const { messages: fetchedMessages, paging } =
      await instagramService.fetchLatestMessages({
        igConversationId,
        accessToken: pageAccessToken,
        limit: 25,
      });

    console.log(`✅ Fetched ${fetchedMessages.length} messages`);

    // STEP 6: Create conversation record
    const lastMessage = buildLastMessageSnapshot(
      fetchedMessages[0],
      businessIgUserId
    );

    const conversation = await Conversation.create({
      platform: "instagram",
      igConversationId: igConversationId,
      creatorId: creatorId,
      participantId: participant._id,
      lastMessage: lastMessage,
      lastActivityAt: new Date(fetchedMessages[0]?.created_time || Date.now()),
      lastSyncedAt: new Date(),
      lastMetaCursor: paging?.cursors?.after || null,
      unreadCount: 0,
      label: "General",
      labelSource: "auto",
    });

    console.log("✅ Conversation created:", conversation._id);

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
 */
async function findConversationIdFromMeta({
  businessIgUserId,
  participantIgUserId,
  pageAccessToken,
}) {
  try {
    let after = null;
    let attempts = 0;
    const maxAttempts = 5; // Check up to 50 conversations (5 pages × 10)

    while (attempts < maxAttempts) {
      const { data, paging } = await instagramService.fetchConversations({
        pageId: businessIgUserId,
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
          console.log("✅ Found conversation:", conv.id);
          return conv.id;
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