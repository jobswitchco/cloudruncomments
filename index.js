async function handlePostback(event) {
  const senderId = event.sender?.id;
  let payload, title;

  if (event.postback) {
    payload = event.postback.payload;
    title = event.postback.title;
  } else if (event.message?.quick_reply) {
    payload = event.message.quick_reply.payload;
    title = event.message.text;
  }

  if (!senderId || !payload) {
    console.warn("⚠️ Missing senderId or payload");
    return;
  }

  console.log("📲 Postback/QuickReply received:", { senderId, payload, title });

  // Helper function to process flow nodes dynamically
  async function processFlowNode(conversation, flowNode) {
    const creds = await ensureFreshPageTokenForUser(conversation.userId);
    const accessToken = creds.fbPageAccessToken;
    const fbPageId = creds.fbPageId;

    if (!accessToken || !fbPageId) {
      throw new Error("Missing credentials");
    }

    conversation.currentFlowId = String(flowNode.id);

    if (flowNode.type === "quickReply") {
      // Build and send quick replies
      const quickReplies = (flowNode.replyOptions || [])
        .slice(0, 13)
        .map((option) => ({
          content_type: "text",
          title: (option.text || "Option").toString().slice(0, 20),
          payload: `QR_${flowNode.id}_${option.id}`,
        }));

      if (quickReplies.length === 0) {
        throw new Error("No quick reply options available");
      }

      await sendFlowMessage({
        recipient: { id: senderId },
        flowNode: {
          type: "quick_replies",
          message: flowNode.config?.quickReplyQuestion || "Choose an option:",
          quick_replies: quickReplies,
        },
        pageAccessToken: accessToken,
        fbPageId,
      });

      await conversation.save();
      console.log("✅ Quick replies sent");
      return;
    }

    if (flowNode.type === "followCheck") {
      // Fetch follow status
      const userDetailsUrl = `${FB_API}/${senderId}`;
      let userDetails = { is_user_follow_business: false };
      try {
        const response = await axios.get(userDetailsUrl, {
          params: {
            access_token: accessToken,
            fields: "id,username,profile_pic,is_user_follow_business,is_business_follow_user",
          },
        });
        userDetails = response.data;
      } catch (err) {
        console.error("❌ Failed to fetch user follow status:", err.message);
      }

      const isFollowing = userDetails.is_user_follow_business === true;
      console.log("🔍 Follow Status Check:", { userId: senderId, isFollowing });

      if (isFollowing) {
        const followingButtons = flowNode.followingButtons || [];
        if (followingButtons.length === 0) {
          conversation.markCompleted();
          await conversation.save();
          return;
        }
        const followingButton = followingButtons[0];
        if (followingButton.actions && followingButton.actions.length > 0) {
          const action = followingButton.actions[0];
          await sendFlowMessage({
            recipient: { id: senderId },
            flowNode: {
              type: "button",
              message: flowNode.config.followCheckYesMessage,
              buttons: [
                {
                  type: "web_url",
                  title: followingButton.text,
                  url: action.config?.redirectUrl || "https://example.com",
                },
              ],
            },
            pageAccessToken: accessToken,
            fbPageId,
          });
        } else {
          await sendFlowMessage({
            recipient: { id: senderId },
            flowNode: {
              type: "text",
              message: flowNode.config.followCheckYesMessage,
            },
            pageAccessToken: accessToken,
            fbPageId,
          });
        }
        conversation.addHistory({
          flowId: String(flowNode.id),
          flowName: "FOLLOW_CHECK_SUCCESS",
          messageSent: flowNode.config.followCheckYesMessage,
          userReply: "Following verified",
          userPayload: null,
        });
        conversation.markCompleted();
        await conversation.save();
        console.log("✅ FollowCheck completed - user is following");
        return;
      } else {
        const notFollowingButtons = flowNode.notFollowingButtons || [];
        if (notFollowingButtons.length === 0) {
          conversation.markCompleted();
          await conversation.save();
          return;
        }
        const verificationButton = notFollowingButtons[0];
        const verificationPayload = `FOLLOWCHECK_RECHECK_${flowNode.id}`;
        await sendFlowMessage({
          recipient: { id: senderId },
          flowNode: {
            type: "button",
            message: flowNode.config.followCheckNoMessage,
            buttons: [
              {
                type: "postback",
                title: verificationButton.text,
                payload: verificationPayload,
              },
            ],
          },
          pageAccessToken: accessToken,
          fbPageId,
        });
        conversation.addHistory({
          flowId: String(flowNode.id),
          flowName: "FOLLOW_CHECK_RETRY",
          messageSent: flowNode.config.followCheckNoMessage,
          userReply: "Not following, retrying",
          userPayload: verificationPayload,
        });
        conversation.currentFlowId = String(flowNode.id);
        await conversation.save();
        console.log("✅ FollowCheck verification button sent");
        return;
      }
    }

    if (flowNode.type === "button" || flowNode.type === "text") {
      await sendFlowMessage({
        recipient: { id: senderId },
        flowNode: flowNode,
        pageAccessToken: accessToken,
        fbPageId,
      });
      conversation.currentFlowId = String(flowNode.id);
      await conversation.save();
      console.log("✅ Button/Text message sent for flow node");
      return;
    }

    throw new Error(`Unsupported flow node type: ${flowNode.type}`);
  }

  // Handle nested quick reply selections with payload prefix QR_NESTED_
  if (payload.startsWith("QR_NESTED_")) {
    console.log("→ Processing nested quick reply selection");

    const conversation = await ConversationState.findOne({
      igUserId: senderId,
      status: "active",
      expiresAt: { $gt: new Date() },
    }).sort({ startedAt: -1 });

    if (!conversation) {
      console.warn("⚠️ No conversation found for nested quick reply");
      return;
    }

    const nestedConfig = conversation.currentNestedQuickReplyConfig;
    if (!nestedConfig) {
      console.warn("⚠️ No nested quick reply config found");
      return;
    }

    const { nestedConfig: config } = nestedConfig;
    const parts = payload.split("_");
    const selectedOptionId = parts[parts.length - 1];

    const selectedNestedOption = config.replyOptions?.find(
      (opt) => String(opt.id) === selectedOptionId
    );

    if (!selectedNestedOption) {
      console.warn("⚠️ Nested option not found:", selectedOptionId);
      return;
    }

    console.log("✅ User selected nested option:", selectedNestedOption.text);

    conversation.addHistory({
      flowId: String(nestedConfig.parentNodeId),
      flowName: "NESTED_QUICK_REPLY",
      messageSent: config.quickReplyQuestion,
      userReply: selectedNestedOption.text,
      userPayload: payload,
    });

    if (selectedNestedOption.actions && selectedNestedOption.actions.length > 0) {
      const action = selectedNestedOption.actions[0];

      const creds = await ensureFreshPageTokenForUser(conversation.userId);
      const accessToken = creds.fbPageAccessToken;
      const fbPageId = creds.fbPageId;

      if (!accessToken || !fbPageId) {
        console.error("❌ Missing credentials");
        conversation.markError(new Error("Missing credentials"));
        await conversation.save();
        return;
      }

      try {
        if (action.type === "redirectLink") {
          const redirectUrl = action.config?.redirectUrl || "https://example.com";
          await sendFlowMessage({
            recipient: { id: senderId },
            flowNode: {
              type: "button",
              message: `You selected: ${selectedNestedOption.text} ✓`,
              buttons: [
                {
                  type: "web_url",
                  title: "Open Link",
                  url: redirectUrl,
                },
              ],
            },
            pageAccessToken: accessToken,
            fbPageId,
          });
          console.log("✅ Nested redirect action sent");
        }
      } catch (err) {
        console.error("❌ Failed to execute nested action:", err.message);
        conversation.markError(err);
        await conversation.save();
        return;
      }
    } else {
      console.log("ℹ️ No actions configured for nested option");
    }
    conversation.markCompleted();
    conversation.currentNestedQuickReplyConfig = null;
    await conversation.save();
    await Automation.updateOne(
      { _id: conversation.automationId },
      { $inc: { "runStats.flowConversationsCompleted": 1 } }
    );
    return;
  }

  // Handle FLOW_START (initial button click)
  if (payload.startsWith("FLOW_START_")) {
    console.log("→ User clicked initial button, opening 24hr window");

    const automationId = payload.replace("FLOW_START_", "");

    const conversation = await ConversationState.findOne({
      igUserId: senderId,
      automationId,
      status: "active",
      expiresAt: { $gt: new Date() },
    }).sort({ startedAt: -1 });

    if (!conversation) {
      console.log("ℹ️ No conversation found");
      return;
    }

    const flowConfig = conversation.flowConfig || [];
    const firstNode = flowConfig[0];

    if (!firstNode) {
      console.error("❌ No flow nodes configured");
      return;
    }

    try {
      await processFlowNode(conversation, firstNode);
      conversation.addHistory({
        flowId: String(firstNode.id),
        flowName: firstNode.type.toUpperCase(),
        messageSent: firstNode.config?.quickReplyQuestion || firstNode.message || "",
        timestamp: new Date(),
      });
    } catch (err) {
      console.error("❌ Failed to send flow node message:", err.message);
      conversation.markError(err);
      await conversation.save();
    }
    return;
  }

  // Main conversation handling after initial
  const conversation = await ConversationState.findOne({
    igUserId: senderId,
    status: "active",
    expiresAt: { $gt: new Date() },
  }).sort({ startedAt: -1 });

  if (!conversation) {
    console.log("ℹ️ No active conversation found for user:", senderId);
    return;
  }

  const currentFlowId = conversation.currentFlowId;
  const flowConfig = conversation.flowConfig || [];

  const currentNode = flowConfig.find((node) => String(node.id) === String(currentFlowId));

  if (!currentNode) {
    console.error("❌ Current node not found:", currentFlowId);
    return;
  }

  if (currentNode.type === "quickReply") {
    console.log("→ Processing Quick Reply from quickReply node");

    const selectedOption = currentNode.replyOptions?.find(
      (opt) =>
        payload === `QR_${currentNode.id}_${opt.id}` ||
        payload === `QR_${currentNode.id}_${currentNode.replyOptions.indexOf(opt)}`
    );

    if (!selectedOption) {
      console.warn("⚠️ Selected option not found for payload:", payload);
      return;
    }

    console.log("✅ User selected:", selectedOption.text);

    conversation.addHistory({
      flowId: String(currentNode.id),
      flowName: "QUICK_REPLY",
      messageSent: currentNode.config.quickReplyQuestion,
      userReply: selectedOption.text,
      userPayload: payload,
    });

    if (selectedOption.actions && selectedOption.actions.length > 0) {
      const action = selectedOption.actions[0];

      const creds = await ensureFreshPageTokenForUser(conversation.userId);
      const accessToken = creds.fbPageAccessToken;
      const fbPageId = creds.fbPageId;

      if (!accessToken || !fbPageId) {
        console.error("❌ Missing credentials");
        conversation.markError(new Error("Missing credentials"));
        await conversation.save();
        return;
      }

      try {
        if (action.type === "quickReply") {
          console.log("→ Executing nested quickReply action");

          const nestedQuickReplyConfig = action.config;

          if (
            !nestedQuickReplyConfig ||
            !nestedQuickReplyConfig.quickReplyQuestion ||
            !nestedQuickReplyConfig.replyOptions
          ) {
            console.warn("⚠️ Nested quick reply config missing or incomplete");
            conversation.markCompleted();
            await conversation.save();
            return;
          }

          await sendFlowMessage({
            recipient: { id: senderId },
            flowNode: {
              type: "quick_replies",
              message: nestedQuickReplyConfig.quickReplyQuestion,
              quick_replies: nestedQuickReplyConfig.replyOptions.map((opt) => ({
                content_type: "text",
                title: opt.text.slice(0, 20),
                payload: `QR_NESTED_${currentNode.id}_${opt.id}`,
              })),
            },
            pageAccessToken: accessToken,
            fbPageId,
          });

          conversation.currentNestedQuickReplyConfig = {
            parentNodeId: String(currentNode.id),
            parentOptionId: String(selectedOption.id),
            nestedConfig: nestedQuickReplyConfig,
          };
          await conversation.save();

          console.log("✅ Nested quick replies sent");
        } else if (action.type === "redirectLink") {
          const redirectUrl = action.config?.redirectUrl || "https://example.com";

          await sendFlowMessage({
            recipient: { id: senderId },
            flowNode: {
              type: "button",
              message: `You selected: ${selectedOption.text} ✓`,
              buttons: [
                {
                  type: "web_url",
                  title: "Open Link",
                  url: redirectUrl,
                },
              ],
            },
            pageAccessToken: accessToken,
            fbPageId,
          });

          console.log("✅ Redirect action sent");
        } else if (action.type === "nextFlow") {
          const nextFlowId = action.config?.nextFlowId;
          if (nextFlowId) {
            const nextNode = flowConfig.find((node) => node.id === nextFlowId);
            if (nextNode) {
              await processFlowNode(conversation, nextNode);
              conversation.addHistory({
                flowId: nextFlowId,
                flowName: nextFlowId,
                messageSent: nextNode.message,
                userReply: title,
                userPayload: payload,
              });
              await conversation.save();
              console.log("✅ Next flow node sent");
            }
          }
        }
      } catch (err) {
        console.error("❌ Failed to execute action:", err.message);
        conversation.markError(err);
        await conversation.save();
        return;
      }
    } else {
      console.log("ℹ️ No actions configured for this option");
    }

    conversation.markCompleted();
    await conversation.save();
    await Automation.updateOne(
      { _id: conversation.automationId },
      { $inc: { "runStats.flowConversationsCompleted": 1 } }
    );

    return;
  }

  if (currentNode.type === "followCheck") {
    console.log("→ Processing FollowCheck verification");

    const creds = await ensureFreshPageTokenForUser(conversation.userId);
    const accessToken = creds.fbPageAccessToken;
    const fbPageId = creds.fbPageId;

    if (!accessToken || !fbPageId) {
      console.error("❌ Missing credentials");
      return;
    }

    const userDetailsUrl = `${FB_API}/${senderId}`;
    let userDetails;
    try {
      const response = await axios.get(userDetailsUrl, {
        params: {
          access_token: accessToken,
          fields: "id,username,profile_pic,is_user_follow_business,is_business_follow_user",
        },
      });
      userDetails = response.data;
    } catch (err) {
      console.error("❌ Failed to fetch user follow status:", err.message);
      userDetails = { is_user_follow_business: false };
    }

    const isFollowing = userDetails.is_user_follow_business === true;

    console.log("🔍 Follow Status Check:", { userId: senderId, isFollowing });

    if (isFollowing) {
      const followingButtons = currentNode.followingButtons || [];
      if (followingButtons.length === 0) {
        console.warn("⚠️ No following buttons configured");
        conversation.markCompleted();
        await conversation.save();
        return;
      }
      const followingButton = followingButtons[0];
      if (followingButton.actions && followingButton.actions.length > 0) {
        const action = followingButton.actions[0];
        try {
          await sendFlowMessage({
            recipient: { id: senderId },
            flowNode: {
              type: "button",
              message: currentNode.config.followCheckYesMessage,
              buttons: [
                {
                  type: "web_url",
                  title: followingButton.text,
                  url: action.config?.redirectUrl || "https://example.com",
                },
              ],
            },
            pageAccessToken: accessToken,
            fbPageId,
          });
          console.log("✅ Following branch button sent");
        } catch (err) {
          console.error("❌ Failed to send following button:", err.message);
        }
      } else {
        try {
          await sendFlowMessage({
            recipient: { id: senderId },
            flowNode: {
              type: "text",
              message: currentNode.config.followCheckYesMessage,
            },
            pageAccessToken: accessToken,
            fbPageId,
          });
          console.log("✅ Following branch message sent");
        } catch (err) {
          console.error("❌ Failed to send message:", err.message);
        }
      }

      conversation.addHistory({
        flowId: String(currentNode.id),
        flowName: "FOLLOW_CHECK_SUCCESS",
        messageSent: currentNode.config.followCheckYesMessage,
        userReply: "Following confirmed",
        userPayload: "FOLLOWING_VERIFIED",
      });

      conversation.markCompleted();
      await conversation.save();
      await Automation.updateOne(
        { _id: conversation.automationId },
        { $inc: { "runStats.flowConversationsCompleted": 1 } }
      );
      return;
    } else {
      const notFollowingButtons = currentNode.notFollowingButtons || [];
      if (notFollowingButtons.length === 0) {
        console.warn("⚠️ No notFollowing buttons configured");
        conversation.markCompleted();
        await conversation.save();
        return;
      }
      const verificationButton = notFollowingButtons[0];
      const verificationPayload = `FOLLOWCHECK_RECHECK_${currentNode.id}`;
      try {
        await sendFlowMessage({
          recipient: { id: senderId },
          flowNode: {
            type: "button",
            message: currentNode.config.followCheckNoMessage,
            buttons: [
              {
                type: "postback",
                title: verificationButton.text,
                payload: verificationPayload,
              },
            ],
          },
          pageAccessToken: accessToken,
          fbPageId,
        });
        console.log("✅ Verification button sent again");
      } catch (err) {
        console.error("❌ Failed to send verification button:", err.message);
      }
      conversation.addHistory({
        flowId: String(currentNode.id),
        flowName: "FOLLOW_CHECK_RETRY",
        messageSent: currentNode.config.followCheckNoMessage,
        userReply: "Not following, retrying",
        userPayload: verificationPayload,
      });
      await conversation.save();
      return;
    }
  }

  // Handle standard flow continuation
  const nextFlowId =
    currentNode.next_actions instanceof Map
      ? currentNode.next_actions.get(payload)
      : currentNode.next_actions?.[payload];

  if (!nextFlowId) {
    console.log("🏁 Conversation completed (no next flow)");
    conversation.markCompleted();
    await conversation.save();
    await Automation.updateOne(
      { _id: conversation.automationId },
      { $inc: { "runStats.flowConversationsCompleted": 1 } }
    );
    return;
  }

  const nextNode = flowConfig.find((node) => node.id === nextFlowId);
  if (!nextNode) {
    console.error("❌ Next node not found:", nextFlowId);
    return;
  }

  const creds = await ensureFreshPageTokenForUser(conversation.userId);
  const accessToken = creds.fbPageAccessToken;
  const fbPageId = creds.fbPageId;

  if (!accessToken || !fbPageId) {
    console.error("❌ Missing credentials");
    return;
  }

  try {
    await sendFlowMessage({
      recipient: { id: String(senderId) },
      flowNode: nextNode,
      pageAccessToken: accessToken,
      fbPageId,
    });
    conversation.addHistory({
      flowId: nextFlowId,
      flowName: nextFlowId,
      messageSent: nextNode.message,
      userReply: title,
      userPayload: payload,
    });
    conversation.currentFlowId = nextFlowId;
    await conversation.save();
    console.log("✅ Next flow node sent");
  } catch (err) {
    console.error("❌ Failed to send next node:", err.message, err.details || err.stack);
    conversation.markError(err);
    await conversation.save();
  }
}
