from typing import List
from datetime import datetime
from fastapi import HTTPException, status

from app.schemas.message import MessageCreate, MessageResponse, PaginatedMessageResponse
from app.models.cassandra_models import MessageModel
from app.models.cassandra_models import ConversationModel

class MessageController:
    """
    Controller for handling message operations
    """

    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        """
        Send a message from one user to another

        Args:
            message_data: The message data including content, sender_id, and receiver_id

        Returns:
            The created message with metadata

        Raises:
            HTTPException: If message sending fails
        """
        try:
            conversation_id = getattr(message_data, "conversation_id", None)
            if not conversation_id:
                conversation = await ConversationModel.create_or_get_conversation(
                    message_data.sender_id, message_data.receiver_id)
                conversation_id = conversation['id']

            # Send the message
            message = await MessageModel.create_message(
                conversation_id=conversation_id,
                sender_id=message_data.sender_id,
                receiver_id=message_data.receiver_id,
                content=message_data.content
            )

            return MessageResponse(
                id=message['id'],
                conversation_id=message['conversation_id'],
                sender_id=message['sender_id'],
                receiver_id=message['receiver_id'],
                content=message['content'],
                created_at=message['created_at'],
                read_at=message.get('read_at')
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to send message: {str(e)}"
            )
    async def get_conversation_messages(
        self,
        conversation_id: int,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get all messages in a conversation with pagination

        Args:
            conversation_id: ID of the conversation
            page: Page number
            limit: Number of messages per page

        Returns:
            Paginated list of messages

        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            # Get messages for this conversation
            result = await MessageModel.get_conversation_messages(
                conversation_id=conversation_id,
                page=page,
                limit=limit
            )

            return PaginatedMessageResponse(
                total=result['total'],
                page=result['page'],
                limit=result['limit'],
                data=result['data']
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get conversation messages: {str(e)}"
            )

    async def get_messages_before_timestamp(
        self,
        conversation_id: int,
        before_timestamp: datetime,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get messages in a conversation before a specific timestamp with pagination

        Args:
            conversation_id: ID of the conversation
            before_timestamp: Get messages before this timestamp
            page: Page number
            limit: Number of messages per page

        Returns:
            Paginated list of messages

        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            # Get messages before timestamp
            result = await MessageModel.get_messages_before_timestamp(
                conversation_id=conversation_id,
                before_timestamp=before_timestamp,
                page=page,
                limit=limit
            )

            return PaginatedMessageResponse(
                total=result['total'],
                page=result['page'],
                limit=result['limit'],
                data=result['data']
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get messages before timestamp: {str(e)}"
            )
