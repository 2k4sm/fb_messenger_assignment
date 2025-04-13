# FB Messenger Backend Implementation with Cassandra

A scalable Facebook Messenger backend implementation using Apache Cassandra as the distributed database and FastAPI as the web framework. This application demonstrates how to design and implement a messaging system that can handle high volumes of messages with efficient data access patterns.

## Architecture

The application follows a clean architecture pattern with separation of concerns:
- **API Layer**: FastAPI routes that define the HTTP endpoints
- **Controller Layer**: Business logic for handling message and conversation operations
- **Model Layer**: Data access layer for interacting with Cassandra
- **Schema Layer**: Pydantic models for request/response validation

### Directory Structure

```
app/
├── api/
│   └── routes/
├── controllers/
├── db/
├── models/
├── schemas/
└── main.py
```

## Cassandra Data Model

The application uses the following Cassandra table structure:

- **messages**: Stores all messages with conversation_id as partition key and timestamp as clustering column
- **messages_by_user**: Indexes messages by user for quick access to a user's messages
- **conversations**: Stores conversation metadata
- **conversations_by_user**: Indexes conversations by user for quick access to a user's conversations

This schema design enables efficient queries for the main access patterns:
1. Fetch messages for a conversation (paginated)
2. Fetch messages before a specific timestamp
3. Fetch a user's recent conversations
4. Fetch details of a specific conversation

## Requirements

- Docker and Docker Compose (for containerized setup)
- Python 3.8+ (for local development)
- Apache Cassandra

## Quick Setup with Docker

Get started quickly with the included Docker setup:

1. Clone this repository
2. Ensure Docker and Docker Compose are installed
3. Run the initialization script:
   ```
   ./init.sh
   ```

This will:
- Start the FastAPI application and Cassandra containers
- Initialize the Cassandra keyspace and tables
- Generate test data for development
- Make the API available at http://localhost:8000

Access the interactive API documentation at http://localhost:8000/docs

To stop the application:
```
docker-compose down
```

## Test Data

The provided test data includes:

- Users with IDs 1-10
- Multiple conversations between users
- Messages in each conversation with realistic timestamps

To regenerate test data:

```
docker-compose exec app python scripts/generate_test_data.py
```

## Exploring Data in Cassandra

To explore the test data:

1. Connect to the Cassandra container:
   ```
   docker-compose exec cassandra cqlsh
   ```

2. Switch to the messenger keyspace:
   ```
   USE messenger;
   ```

3. Sample queries:
   ```
   -- View conversations
   SELECT * FROM conversations LIMIT 10;

   -- View messages in conversation 1
   SELECT * FROM messages WHERE conversation_id = 1 LIMIT 20;

   -- View conversations for user 1
   SELECT * FROM conversations_by_user WHERE user_id = 1;
   ```

## Manual Setup

For a non-Docker setup:

1. Install Cassandra locally and start it
2. Create a Python virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
4. Initialize Cassandra:
   ```
   python scripts/setup_db.py
   ```
5. Start the application:
   ```
   uvicorn app.main:app --reload
   ```

## API Endpoints

### Messages

- **Send a message**
  - `POST /api/messages/`
  - Body: `{"sender_id": int, "receiver_id": int, "content": string, "conversation_id": int}`

- **Get conversation messages**
  - `GET /api/messages/conversation/{conversation_id}?page={page}&limit={limit}`
  - Retrieve paginated messages for a specific conversation

- **Get messages before timestamp**
  - `GET /api/messages/conversation/{conversation_id}/before?before_timestamp={timestamp}&page={page}&limit={limit}`
  - Retrieve messages before a specific timestamp

### Conversations

- **Get user conversations**
  - `GET /api/conversations/user/{user_id}?page={page}&limit={limit}`
  - Retrieve paginated list of conversations for a user

- **Get conversation details**
  - `GET /api/conversations/{conversation_id}`
  - Retrieve details of a specific conversation

## Implementation Highlights

- **Efficient pagination**: Implemented with LIMIT and offset pattern
- **Time-based queries**: Messages can be retrieved before a specific timestamp
- **Error handling**: Comprehensive error handling for all endpoints
- **Cassandra optimizations**:
  - Tables designed with appropriate partition keys for efficient data distribution
  - Clustering columns for ordered data retrieval
  - Denormalized data to optimize for read patterns

## Schema design

### Keyspaces

```cql
CREATE KEYSPACE IF NOT EXISTS messenger
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
};

USE messenger;
```

### Tables

### 1. Users Table
Stores user details

```cql
CREATE TABLE IF NOT EXISTS users (
    user_id int,
    username text,
    created_at timestamp,
    PRIMARY KEY (user_id)
)
```

### 2. Messages Table
Stores all messages with the conversation ID as the partition key for efficient retrieval of conversation messages.

```cql
CREATE TABLE messages (
  conversation_id INT,
  timestamp TIMESTAMP,
  message_id UUID,
  sender_id INT,
  receiver_id INT,
  content TEXT,
  read_at TIMESTAMP,
  PRIMARY KEY ((conversation_id), timestamp, message_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, message_id ASC);
```

### 3. Messages By User Table
Allows querying messages by user ID for message history.

```cql
CREATE TABLE messages_by_user (
  user_id INT,
  conversation_id INT,
  timestamp TIMESTAMP,
  message_id UUID,
  sender_id INT,
  receiver_id INT,
  content TEXT,
  PRIMARY KEY ((user_id), conversation_id, timestamp, message_id)
) WITH CLUSTERING ORDER BY (conversation_id ASC, timestamp DESC, message_id ASC);
```

### 4. Conversations Table
Stores conversation metadata.

```cql
CREATE TABLE conversations (
  conversation_id INT,
  user1_id INT,
  user2_id INT,
  created_at TIMESTAMP,
  last_message_at TIMESTAMP,
  last_message_content TEXT,
  PRIMARY KEY (conversation_id)
);
```

### 5. Conversations By User Table
Allows efficient retrieval of all conversations for a specific user.

```cql
CREATE TABLE conversations_by_user (
  user_id INT,
  conversation_id INT,
  other_user_id INT,
  last_message_at TIMESTAMP,
  last_message_content TEXT,
  PRIMARY KEY ((user_id), last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC);
```

## Key Design Principles

1. **Denormalization**: The schema duplicates data across tables to optimize for read performance.

2. **Partition Keys**: Designed based on query patterns:
   - `conversation_id` for retrieving all messages in a conversation
   - `user_id` for retrieving all messages or conversations for a user

3. **Clustering Keys**:
   - `timestamp` in descending order to retrieve the most recent messages first
   - `message_id` to ensure uniqueness when timestamps collide

This schema supports all the operations in the codebase including:
- Sending messages
- Retrieving conversation messages
- Retrieving messages before a timestamp
- Getting user conversations
- Getting conversation details
