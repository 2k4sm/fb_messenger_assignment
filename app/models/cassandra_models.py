"""
Models for interacting with Cassandra tables.
"""
import uuid
from datetime import datetime
from typing import Dict, Any, Optional

from uuid import UUID

from app.db.cassandra_client import cassandra_client

class MessageModel:
    """
    Message model for interacting with the messages table.
    """

    @staticmethod
    async def create_message(
        sender_id: UUID,
        receiver_id: UUID,
        content: str,
        conversation_id: int
    ) -> Dict[str, Any]:
        """
        Create a new message.

        Args:
            sender_id: ID of the sender
            receiver_id: ID of the receiver
            content: Content of the message
            conversation_id: ID of the conversation

        Returns:
            Dictionary with message data
        """
        timestamp = datetime.utcnow()
        message_id = uuid.uuid4()

        sender_id_str = str(sender_id)
        receiver_id_str = str(receiver_id)
        message_id_str = str(message_id)

        cassandra_client.execute(
            """
            INSERT INTO messages (
                conversation_id, timestamp, message_id, sender_id, receiver_id, content
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [conversation_id, timestamp, message_id_str, sender_id_str, receiver_id_str, content]
        )

        for user_id in [sender_id_str, receiver_id_str]:
            cassandra_client.execute(
                """
                INSERT INTO messages_by_user (
                    user_id, conversation_id, timestamp, message_id, sender_id, receiver_id, content
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                [user_id, conversation_id, timestamp, message_id_str, sender_id_str, receiver_id_str, content]
            )

        cassandra_client.execute(
            """
            UPDATE conversations SET
                last_message_at = %s,
                last_message_content = %s
            WHERE conversation_id = %s
            """,
            [timestamp, content, conversation_id]
        )

        for user_id in [sender_id_str, receiver_id_str]:
            other_user_id = receiver_id_str if user_id == sender_id_str else sender_id_str
            cassandra_client.execute(
                """
                INSERT INTO conversations_by_user (
                    user_id, conversation_id, other_user_id, last_message_at, last_message_content
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                [user_id, conversation_id, other_user_id, timestamp, content]
            )

        return {
            'id': message_id,
            'conversation_id': conversation_id,
            'sender_id': sender_id,
            'receiver_id': receiver_id,
            'content': content,
            'created_at': timestamp,
            'read_at': None
        }

    @staticmethod
    async def get_conversation_messages(
        conversation_id: int,
        page: int = 1,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get messages for a conversation with pagination.

        Args:
            conversation_id: ID of the conversation
            page: Page number
            limit: Number of messages per page

        Returns:
            Dictionary with total count and messages list
        """
        offset = (page - 1) * limit

        count_query = """
        SELECT COUNT(*) as count FROM messages
        WHERE conversation_id = %s
        """
        count_result = cassandra_client.execute(count_query, [conversation_id])
        total = count_result[0]['count'] if count_result else 0

        query = """
        SELECT conversation_id, timestamp, message_id, sender_id, receiver_id, content
        FROM messages
        WHERE conversation_id = %s
        LIMIT %s
        """

        if page > 1:
            messages = []
            result = cassandra_client.execute(query, [conversation_id, limit * page])
            messages = list(result)[offset:offset+limit]
        else:
            result = cassandra_client.execute(query, [conversation_id, limit])
            messages = list(result)

        formatted_messages = [{
            'id': msg['message_id'],
            'conversation_id': msg['conversation_id'],
            'sender_id': msg['sender_id'],
            'receiver_id': msg['receiver_id'],
            'content': msg['content'],
            'created_at': msg['timestamp']
        } for msg in messages]

        return {
            'total': total,
            'page': page,
            'limit': limit,
            'data': formatted_messages
        }

    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: int,
        before_timestamp: datetime,
        page: int = 1,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get messages before a timestamp with pagination.

        Args:
            conversation_id: ID of the conversation
            before_timestamp: Get messages before this timestamp
            page: Page number
            limit: Number of messages per page

        Returns:
            Dictionary with total count and messages list
        """
        offset = (page - 1) * limit

        count_query = """
        SELECT COUNT(*) as count FROM messages
        WHERE conversation_id = %s AND timestamp < %s
        ALLOW FILTERING
        """
        count_result = cassandra_client.execute(count_query, [conversation_id, before_timestamp])
        total = count_result[0]['count'] if count_result else 0

        query = """
        SELECT conversation_id, timestamp, message_id, sender_id, receiver_id, content
        FROM messages
        WHERE conversation_id = %s AND timestamp < %s
        LIMIT %s
        ALLOW FILTERING
        """

        if page > 1:
            messages = []
            result = cassandra_client.execute(query, [conversation_id, before_timestamp, limit * page])
            messages = list(result)[offset:offset+limit]
        else:
            result = cassandra_client.execute(query, [conversation_id, before_timestamp, limit])
            messages = list(result)

        formatted_messages = [{
            'id': msg['message_id'],
            'conversation_id': msg['conversation_id'],
            'sender_id': msg['sender_id'],
            'receiver_id': msg['receiver_id'],
            'content': msg['content'],
            'created_at': msg['timestamp']
        } for msg in messages]

        return {
            'total': total,
            'page': page,
            'limit': limit,
            'data': formatted_messages
        }

class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    """

    @staticmethod
    async def get_user_conversations(
        user_id: UUID,
        page: int = 1,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get conversations for a user with pagination.

        Args:
            user_id: ID of the user
            page: Page number
            limit: Number of conversations per page

        Returns:
            Dictionary with total count and conversations list
        """
        offset = (page - 1) * limit

        count_query = """
        SELECT COUNT(*) as count FROM conversations_by_user
        WHERE user_id = %s
        """
        count_result = cassandra_client.execute(count_query, [user_id])
        total = count_result[0]['count'] if count_result else 0

        query = """
        SELECT user_id, conversation_id, other_user_id, last_message_at, last_message_content
        FROM conversations_by_user
        WHERE user_id = %s
        LIMIT %s
        """

        if page > 1:
            result = cassandra_client.execute(query, [user_id, limit * page])
            conversations = list(result)[offset:offset+limit]
        else:
            result = cassandra_client.execute(query, [user_id, limit])
            conversations = list(result)

        formatted_conversations = []
        for conv in conversations:
            conv_detail = cassandra_client.execute(
                "SELECT * FROM conversations WHERE conversation_id = %s",
                [conv['conversation_id']]
            )
            if conv_detail:
                detail = conv_detail[0]
                formatted_conversations.append({
                    'id': detail['conversation_id'],
                    'user1_id': detail['user1_id'],
                    'user2_id': detail['user2_id'],
                    'last_message_at': detail['last_message_at'],
                    'last_message_content': detail['last_message_content']
                })

        return {
            'total': total,
            'page': page,
            'limit': limit,
            'data': formatted_conversations
        }

    @staticmethod
    async def get_conversation(conversation_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a conversation by ID.

        Args:
            conversation_id: ID of the conversation

        Returns:
            Conversation details or None if not found
        """
        query = "SELECT * FROM conversations WHERE conversation_id = %s"
        result = cassandra_client.execute(query, [conversation_id])

        if not result:
            return None

        conv = result[0]
        return {
            'id': conv['conversation_id'],
            'user1_id': conv['user1_id'],
            'user2_id': conv['user2_id'],
            'last_message_at': conv['last_message_at'],
            'last_message_content': conv['last_message_content']
        }

    @staticmethod
    async def create_or_get_conversation(user1_id: UUID, user2_id: UUID) -> Dict[str, Any]:
        """
        Get an existing conversation between two users or create a new one.

        Args:
            user1_id: ID of the first user
            user2_id: ID of the second user

        Returns:
            Conversation details
        """
        user1_id_str = str(user1_id)
        user2_id_str = str(user2_id)

        query = """
        SELECT * FROM conversations
        WHERE (user1_id = %s AND user2_id = %s) OR (user1_id = %s AND user2_id = %s)
        ALLOW FILTERING
        """
        result = cassandra_client.execute(query, [user1_id_str, user2_id_str, user2_id_str, user1_id_str])

        if result:
            # Conversation exists
            conv = result[0]
            return {
                'id': conv['conversation_id'],
                'user1_id': conv['user1_id'],
                'user2_id': conv['user2_id'],
                'created_at': conv['created_at'],
                'last_message_at': conv['last_message_at'],
                'last_message_content': conv['last_message_content']
            }

        count_query = "SELECT COUNT(*) as count FROM conversations"
        count_result = cassandra_client.execute(count_query, [])
        new_id = count_result[0]['count'] + 1 if count_result else 1

        now = datetime.utcnow()

        cassandra_client.execute(
            """
            INSERT INTO conversations (
                conversation_id, user1_id, user2_id, created_at, last_message_at
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            [new_id, user1_id_str, user2_id_str, now, now]
        )

        return {
            'id': new_id,
            'user1_id': user1_id,
            'user2_id': user2_id,
            'created_at': now,
            'last_message_at': now,
            'last_message_content': None
        }
