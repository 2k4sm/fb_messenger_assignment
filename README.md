# FB Messenger Backend Implementation with Cassandra

A scalable Facebook Messenger backend implementation using Apache Cassandra as the distributed database and FastAPI as the web framework. This application demonstrates how to design and implement a messaging system that can handle high volumes of messages with efficient data access patterns.

## Architecture

The application uses a layered architecture:
- **API Layer**: FastAPI endpoints
- **Controller Layer**: Business logic
- **Model Layer**: Data access for Cassandra
- **Schema Layer**: Pydantic models for validation

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

The application uses these Cassandra tables:

- **messages**: Messages stored by conversation_id
- **messages_by_user**: Messages indexed by user
- **conversations**: Conversation metadata
- **conversations_by_user**: Conversations indexed by user

This design enables efficient queries for:
- Fetching conversation messages with pagination
- Retrieving messages before a specific timestamp
- Getting a user's conversations
- Accessing conversation details

## Setup Instructions

### Prerequisites
- Docker and Docker Compose
- Python 3.11 (for local development)

### Docker Setup (Recommended)

1. Clone the repository
2. Run:
   ```
   ./init.sh
   ```

This will:
- Start the application and Cassandra containers
- Initialize the database
- Load test data
- Make the API available at http://localhost:8000

API documentation: http://localhost:8000/docs

To stop:
```
docker-compose down
```

### Manual Setup

1. Install and start Cassandra
2. Set up Python environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Initialize database:
   ```
   python scripts/setup_db.py
   ```
4. Start the application:
   ```
   uvicorn app.main:app --reload
   ```

## Working with Test Data

The test data includes users, conversations and messages with realistic data.

To regenerate test data:
```
docker-compose exec app python scripts/generate_test_data.py
```

### Exploring Data

Connect to Cassandra:
```
docker-compose exec cassandra cqlsh
```

Sample queries:
```
USE messenger;

-- View conversations
SELECT * FROM conversations LIMIT 10;

-- View messages in a conversation
SELECT * FROM messages WHERE conversation_id = 1 LIMIT 20;

-- View a user's conversations
SELECT * FROM conversations_by_user WHERE user_id = 3e3ab267-62b8-4681-a3eb-e04245156671;
```

## API Endpoints

### Messages

- **Send a message**
  - `POST /api/messages/`
  - Body: `{"sender_id": uuid, "receiver_id": uuid, "content": string, "conversation_id": int}`

- **Get conversation messages**
  - `GET /api/messages/conversation/{conversation_id}?page={page}&limit={limit}`

- **Get messages before timestamp**
  - `GET /api/messages/conversation/{conversation_id}/before?before_timestamp={timestamp}&page={page}&limit={limit}`

### Conversations

- **Get user conversations**
  - `GET /api/conversations/user/{user_id}?page={page}&limit={limit}`

- **Get conversation details**
  - `GET /api/conversations/{conversation_id}`

## Database Schema

### Keyspace

```cql
CREATE KEYSPACE IF NOT EXISTS messenger
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
};
```

### Tables

**Users**
```cql
CREATE TABLE IF NOT EXISTS users (
    user_id uuid,
    username text,
    created_at timestamp,
    PRIMARY KEY (user_id)
)
```

**Messages**
```cql
CREATE TABLE messages (
  conversation_id INT,
  timestamp TIMESTAMP,
  message_id UUID,
  sender_id uuid,
  receiver_id uuid,
  content TEXT,
  read_at TIMESTAMP,
  PRIMARY KEY ((conversation_id), timestamp, message_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, message_id ASC);
```

**Messages By User**
```cql
CREATE TABLE messages_by_user (
  user_id uuid,
  conversation_id INT,
  timestamp TIMESTAMP,
  message_id UUID,
  sender_id uuid,
  receiver_id uuid,
  content TEXT,
  PRIMARY KEY ((user_id), conversation_id, timestamp, message_id)
) WITH CLUSTERING ORDER BY (conversation_id ASC, timestamp DESC, message_id ASC);
```

**Conversations**
```cql
CREATE TABLE conversations (
  conversation_id INT,
  user1_id uuid,
  user2_id uuid,
  created_at TIMESTAMP,
  last_message_at TIMESTAMP,
  last_message_content TEXT,
  PRIMARY KEY (conversation_id)
);
```

**Conversations By User**
```cql
CREATE TABLE conversations_by_user (
  user_id uuid,
  conversation_id INT,
  other_user_id uuid,
  last_message_at TIMESTAMP,
  last_message_content TEXT,
  PRIMARY KEY ((user_id), last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC);
```

## Design Principles

1. **Data Denormalization**: Data is duplicated across tables to optimize read performance
2. **Partition Keys**: Selected based on query patterns
3. **Clustering Columns**: Ordered to support efficient data retrieval
