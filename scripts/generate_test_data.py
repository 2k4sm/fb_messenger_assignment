"""
Script to generate test data for the Messenger application.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def tables_exist(session):
    """Check if required tables exist in the keyspace."""
    logger.info("Checking if required tables exist...")

    required_tables = ['users', 'messages', 'messages_by_user', 'conversations', 'conversations_by_user']

    keyspace_metadata = session.cluster.metadata.keyspaces[CASSANDRA_KEYSPACE]
    existing_tables = keyspace_metadata.tables.keys()

    missing_tables = [table for table in required_tables if table not in existing_tables]

    if missing_tables:
        logger.info(f"Missing tables: {', '.join(missing_tables)}")
        return False

    logger.info("All required tables exist.")
    return True

def create_tables(session):
    """Create the necessary tables for the application."""
    logger.info("Creating tables...")

    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id uuid,
            username text,
            created_at timestamp,
            PRIMARY KEY (user_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            conversation_id int,
            timestamp timestamp,
            message_id uuid,
            sender_id uuid,
            receiver_id uuid,
            content text,
            PRIMARY KEY (conversation_id, timestamp, message_id)
        ) WITH CLUSTERING ORDER BY (timestamp DESC, message_id ASC)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS messages_by_user (
            user_id uuid,
            conversation_id int,
            timestamp timestamp,
            message_id uuid,
            sender_id uuid,
            receiver_id uuid,
            content text,
            PRIMARY KEY ((user_id), conversation_id, timestamp, message_id)
        ) WITH CLUSTERING ORDER BY (conversation_id ASC, timestamp DESC, message_id ASC)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations_by_user (
            user_id uuid,
            conversation_id int,
            other_user_id uuid,
            last_message_at timestamp,
            last_message_content text,
            PRIMARY KEY (user_id, last_message_at, conversation_id)
        ) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            conversation_id int,
            user1_id uuid,
            user2_id uuid,
            created_at timestamp,
            last_message_at timestamp,
            last_message_content text,
            PRIMARY KEY (conversation_id)
        )
    """)

    logger.info("Tables created successfully.")

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    """
    logger.info("Generating test data...")

    logger.info("Creating users...")
    user_ids = []
    for i in range(1, NUM_USERS + 1):
        user_id = uuid.uuid4()
        user_ids.append(user_id)
        username = f"user{i}"
        created_at = datetime.utcnow() - timedelta(days=random.randint(1, 30))

        session.execute(
            """
            INSERT INTO users (user_id, username, created_at)
            VALUES (%s, %s, %s)
            """,
            (user_id, username, created_at)
        )

    logger.info("Creating conversations...")
    conversations = []

    for conv_id in range(1, NUM_CONVERSATIONS + 1):
        user_pair = random.sample(user_ids, 2)
        user1_id, user2_id = user_pair

        created_at = datetime.utcnow() - timedelta(days=random.randint(1, 20))
        last_message_at = created_at
        last_message_content = None

        session.execute(
            """
            INSERT INTO conversations
            (conversation_id, user1_id, user2_id, created_at, last_message_at, last_message_content)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (conv_id, user1_id, user2_id, created_at, last_message_at, last_message_content)
        )

        conversations.append((conv_id, user1_id, user2_id, created_at))

    logger.info("Creating messages...")

    message_contents = [
        "Hey, how are you?",
        "What's up?",
        "Can we meet tomorrow?",
        "I'm busy right now",
        "Let's catch up soon",
        "Did you see that movie?",
        "Have you done the assignment?",
        "I'll call you later",
        "Thanks for your help!",
        "Congratulations!"
    ]

    for conv_id, user1_id, user2_id, created_at in conversations:
        num_messages = random.randint(5, MAX_MESSAGES_PER_CONVERSATION)

        current_time = created_at
        latest_content = None

        for i in range(num_messages):
            current_time += timedelta(minutes=random.randint(1, 60))

            sender_id = user1_id if i % 2 == 0 else user2_id
            receiver_id = user2_id if i % 2 == 0 else user1_id

            content = random.choice(message_contents)
            latest_content = content

            message_id = uuid.uuid4()

            session.execute(
                """
                INSERT INTO messages
                (conversation_id, timestamp, message_id, sender_id, receiver_id, content)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (conv_id, current_time, message_id, sender_id, receiver_id, content)
            )

            for user_id in [sender_id, receiver_id]:
                session.execute(
                    """
                    INSERT INTO messages_by_user
                    (user_id, conversation_id, timestamp, message_id, sender_id, receiver_id, content)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (user_id, conv_id, current_time, message_id, sender_id, receiver_id, content)
                )

        session.execute(
            """
            UPDATE conversations
            SET last_message_at = %s, last_message_content = %s
            WHERE conversation_id = %s
            """,
            (current_time, latest_content, conv_id)
        )

        for user_id in [user1_id, user2_id]:
            other_user_id = user2_id if user_id == user1_id else user1_id
            session.execute(
                """
                INSERT INTO conversations_by_user
                (user_id, conversation_id, other_user_id, last_message_at, last_message_content)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (user_id, conv_id, other_user_id, current_time, latest_content)
            )

    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info("Created test users with UUIDs")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None

    try:
        cluster, session = connect_to_cassandra()

        if not tables_exist(session):
            create_tables(session)

        generate_test_data(session)
        logger.info("Test data generation completed successfully!")
    except Exception as e:
            logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main()
