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

        sender_id_str = sender_id
        receiver_id_str = receiver_id
        message_id_str = message_id

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
        """
        query = """
        SELECT conversation_id, timestamp, message_id, sender_id, receiver_id, content
        FROM messages
        WHERE conversation_id = %s
        """
        result = cassandra_client.execute(query, [conversation_id])
        all_messages = list(result)
        total = len(all_messages)

        all_messages.sort(key=lambda msg: msg['timestamp'], reverse=True)

        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_messages = all_messages[start_idx:end_idx]

        formatted_messages = [{
            'id': msg['message_id'],
            'conversation_id': msg['conversation_id'],
            'sender_id': msg['sender_id'],
            'receiver_id': msg['receiver_id'],
            'content': msg['content'],
            'created_at': msg['timestamp']
        } for msg in paginated_messages]

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
        """
        query = """
        SELECT conversation_id, timestamp, message_id, sender_id, receiver_id, content
        FROM messages
        WHERE conversation_id = %s
        """
        result = cassandra_client.execute(query, [conversation_id])

        filtered_messages = [msg for msg in result if msg['timestamp'] < before_timestamp]
        total = len(filtered_messages)

        filtered_messages.sort(key=lambda msg: msg['timestamp'], reverse=True)

        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_messages = filtered_messages[start_idx:end_idx]

        formatted_messages = [{
            'id': msg['message_id'],
            'conversation_id': msg['conversation_id'],
            'sender_id': msg['sender_id'],
            'receiver_id': msg['receiver_id'],
            'content': msg['content'],
            'created_at': msg['timestamp']
        } for msg in paginated_messages]

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
        """

        query = """
        SELECT user_id, conversation_id, other_user_id, last_message_at, last_message_content
        FROM conversations_by_user
        WHERE user_id = %s
        """
        result = cassandra_client.execute(query, [user_id])
        all_conversations = list(result)
        total = len(all_conversations)

        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_conversations = all_conversations[start_idx:end_idx]

        formatted_conversations = []
        for conv in paginated_conversations:
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
        """
        user1_id_str = user1_id
        user2_id_str = user2_id

        query1 = """
        SELECT * FROM conversations
        WHERE user1_id = %s AND user2_id = %s
        ALLOW FILTERING
        """
        result = cassandra_client.execute(query1, [user1_id_str, user2_id_str])

        if not result:
            # Try the reverse combination
            query2 = """
            SELECT * FROM conversations
            WHERE user1_id = %s AND user2_id = %s
            ALLOW FILTERING
            """
            result = cassandra_client.execute(query2, [user2_id_str, user1_id_str])

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


        id_query = "SELECT MAX(conversation_id) as max_id FROM conversations"
        id_result = cassandra_client.execute(id_query)
        new_id = (id_result[0]['max_id'] or 0) + 1

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
