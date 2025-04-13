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
            user_id UUID PRIMARY KEY,
            username TEXT,
            email TEXT,
            password_hash TEXT,
            created_at TIMESTAMP,
            last_login TIMESTAMP
        )
    """)

    session.execute("CREATE INDEX IF NOT EXISTS ON users (username)")

    session.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            chat_id UUID,
            message_id TIMEUUID,
            sender_id UUID,
            content TEXT,
            sent_at TIMESTAMP,
            is_read BOOLEAN,
            PRIMARY KEY ((chat_id), message_id)
        ) WITH CLUSTERING ORDER BY (message_id DESC)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS user_chats (
            user_id UUID,
            chat_id UUID,
            chat_name TEXT,
            last_updated TIMESTAMP,
            PRIMARY KEY (user_id, chat_id)
        ) WITH CLUSTERING ORDER BY (chat_id ASC)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS chat_participants (
            chat_id UUID,
            user_id UUID,
            joined_at TIMESTAMP,
            PRIMARY KEY (chat_id, user_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS user_connections (
            user_id UUID,
            connection_id UUID,
            connection_type TEXT,
            established_at TIMESTAMP,
            PRIMARY KEY (user_id, connection_id)
        )
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
