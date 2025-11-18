# Test Examples for py-mongo-libs

This folder contains example scripts demonstrating how to use the MongoDB library.

## Files

- **`example_mongodb_operations.py`**: Comprehensive example showing:
  - Connection methods
  - DDL operations (CREATE, DROP, LIST collections and indexes)
  - DML operations (INSERT, UPDATE, DELETE, SELECT)
  - Advanced query operations

- **`test_setup.py`**: Test script to verify the library setup works correctly

## Prerequisites

1. **MongoDB Server**: Make sure MongoDB is installed and running
   - Default connection: `mongodb://localhost:27017`
   - You can download MongoDB from: https://www.mongodb.com/try/download/community

2. **Python Dependencies**: Install required packages
   ```bash
   pip install pymongo
   ```

## Running the Examples

### Test Setup

First, verify that your setup works:

```bash
python test/test_setup.py
```

This will:
- Test that all modules can be imported
- Test connection object creation
- Test actual MongoDB connectivity
- Test basic collection operations
- Test context manager functionality

### Run Examples

Run the comprehensive examples:

```bash
python test/example_mongodb_operations.py
```

This will demonstrate:
1. Different ways to connect to MongoDB
2. DDL operations (creating/dropping collections and indexes)
3. DML operations (insert, update, delete, find)
4. Advanced query operations

## Connection Examples

### Method 1: Using Connection String (DSN)
```python
from src.core.connection import connect

conn = connect(dsn="mongodb://localhost:27017/testdb")
```

### Method 2: Using Individual Parameters
```python
from src.core.connection import connect

conn = connect(
    host="localhost",
    port=27017,
    database="testdb"
)
```

### Method 3: Using Context Manager (Recommended)
```python
from src.core.connection import connect

with connect(host="localhost", port=27017, database="testdb") as conn:
    collection = conn.collection("users")
    # Your operations here
```

## DDL Operations

### Create Collection
```python
db = conn.database
collection = db.create_collection("users")
```

### Create Index
```python
collection.create_index("email", unique=True)
collection.create_index([("name", 1), ("age", -1)])
```

### List Collections
```python
collections = db.list_collection_names()
```

### Drop Collection
```python
db.drop_collection("users")
```

## DML Operations

### Insert
```python
# Insert one
collection.insert_one({"name": "Alice", "age": 30})

# Insert many
collection.insert_many([
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35}
])
```

### Find
```python
# Find one
doc = collection.find_one({"name": "Alice"})

# Find many
cursor = collection.find({"age": {"$gt": 25}})
docs = list(cursor)
```

### Update
```python
# Update one
collection.update_one(
    {"name": "Alice"},
    {"$set": {"age": 31}}
)

# Update many
collection.update_many(
    {"city": "New York"},
    {"$inc": {"age": 1}}
)
```

### Delete
```python
# Delete one
collection.delete_one({"name": "Alice"})

# Delete many
collection.delete_many({"age": {"$lt": 30}})
```

## Troubleshooting

### Connection Errors

If you get connection errors:
1. Make sure MongoDB is running: `mongod` or check your MongoDB service
2. Verify the connection string/parameters are correct
3. Check firewall settings
4. Verify MongoDB is listening on the expected port (default: 27017)

### Import Errors

If you get import errors:
1. Make sure you're running from the project root directory
2. Verify the `src` directory structure is correct
3. Check that `pymongo` is installed: `pip install pymongo`

### Authentication Errors

If you need authentication:
```python
conn = connect(
    host="localhost",
    port=27017,
    database="testdb",
    username="your_username",
    password="your_password",
    auth_source="admin"
)
```

