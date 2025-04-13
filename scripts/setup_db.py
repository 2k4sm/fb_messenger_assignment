"""
Script to initialize Cassandra keyspace and tables for the Messenger application.
"""
import os
import time
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")
CASSANDRA_USER = os.getenv("CASSANDRA_USER", None)
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", None)

def wait_for_cassandra():
    """Wait for Cassandra to be ready before proceeding."""
    logger.info("Waiting for Cassandra to be ready...")
    cluster = None

    for _ in range(10):  # Try 10 times
        try:
            auth_provider = None
            if CASSANDRA_USER and CASSANDRA_PASSWORD:
                auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)

            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
            session = cluster.connect()
            rows = session.execute("SELECT release_version FROM system.local")
            logger.info(f"Cassandra is ready! Version: {rows[0].release_version}")
            return cluster
        except Exception as e:
            logger.warning(f"Cassandra not ready yet: {str(e)}")
            time.sleep(5)  # Wait 5 seconds before trying again

    logger.error("Failed to connect to Cassandra after multiple attempts.")
    raise Exception("Could not connect to Cassandra")

def create_keyspace(session):
    """
    Create the keyspace if it doesn't exist.
    """
    logger.info(f"Creating keyspace {CASSANDRA_KEYSPACE} if it doesn't exist...")

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH REPLICATION = {
            'class' : 'SimpleStrategy',
            'replication_factor' : 3
        }
    """ % CASSANDRA_KEYSPACE)

    logger.info(f"Keyspace {CASSANDRA_KEYSPACE} is ready.")

def create_tables(session):
    """
    Create the tables for the messenger application.
    """
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

    logger.info("Tables created successfully.")

def main():
    """Initialize the database."""
    logger.info("Starting Cassandra initialization...")

    cluster = wait_for_cassandra()

    try:
        session = cluster.connect()

        create_keyspace(session)
        session.set_keyspace(CASSANDRA_KEYSPACE)
        create_tables(session)

        logger.info("Cassandra initialization completed successfully.")
    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        raise
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main()
