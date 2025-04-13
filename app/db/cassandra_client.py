"""
Cassandra client for the Messenger application.
This provides a connection to the Cassandra database.
"""
import os
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import time

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory

logger = logging.getLogger(__name__)

class CassandraClient:
    """Singleton Cassandra client for the application."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CassandraClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize the Cassandra connection."""
        if self._initialized:
            return

        self.host = os.getenv("CASSANDRA_HOST", "localhost")
        self.port = int(os.getenv("CASSANDRA_PORT", "9042"))
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "messenger")
        self.max_retries = int(os.getenv("CASSANDRA_MAX_RETRIES", "30"))
        self.retry_delay = int(os.getenv("CASSANDRA_RETRY_DELAY", "5"))

        self.cluster = None
        self.session = None

        # Initialize but don't immediately fail if connection fails
        # Connection will be established lazily when needed
        self._initialized = True

    def connect_with_retry(self) -> None:
        """Connect to Cassandra with retry mechanism."""
        retries = 0
        while True:  # Keep trying indefinitely until connection is established
            try:
                self.connect()
                logger.info("Successfully connected to Cassandra after retries")
                return
            except Exception as e:
                retries += 1
                logger.warning(f"Failed to connect to Cassandra (attempt {retries}): {str(e)}")
                logger.info(f"Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)

    def connect(self) -> None:
        """Connect to the Cassandra cluster."""
        try:
            logger.info(f"Connecting to Cassandra at {self.host}:{self.port}...")
            self.cluster = Cluster([self.host], port=self.port)
            self.session = self.cluster.connect(self.keyspace)
            self.session.row_factory = dict_factory
            logger.info(f"Connected to Cassandra at {self.host}:{self.port}, keyspace: {self.keyspace}")
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {str(e)}")
            raise

    def close(self) -> None:
        """Close the Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")

    def execute(self, query: str, params: list = None) -> List[Dict[str, Any]]:
        """
        Execute a CQL query.

        Args:
            query: The CQL query string
            params: The parameters for the query as a list (for positional parameters)

        Returns:
            List of rows as dictionaries
        """
        if not self.session:
            self.connect_with_retry()
            if not self.session:
                raise Exception("No Cassandra session available")

        try:
            statement = SimpleStatement(query)
            result = self.session.execute(statement, params or [])
            return list(result)
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise

    def execute_async(self, query: str, params: list = None):
        """
        Execute a CQL query asynchronously.

        Args:
            query: The CQL query string
            params: The parameters for the query as a list (for positional parameters)

        Returns:
            Async result object
        """
        if not self.session:
            self.connect_with_retry()
            if not self.session:
                raise Exception("No Cassandra session available")

        try:
            statement = SimpleStatement(query)
            return self.session.execute_async(statement, params or [])
        except Exception as e:
            logger.error(f"Async query execution failed: {str(e)}")
            raise

    def get_session(self) -> Session:
        """Get the Cassandra session."""
        if not self.session:
            self.connect_with_retry()
            if not self.session:
                raise Exception("No Cassandra session available")
        return self.session

cassandra_client = CassandraClient()
