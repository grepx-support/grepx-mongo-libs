"""
Simple example demonstrating MongoDB connection, DML, and DDL operations
using the py-mongo-libs library.

DML (Data Manipulation Language) operations:
- INSERT: insert_one, insert_many
- UPDATE: update_one, update_many, replace_one
- DELETE: delete_one, delete_many
- SELECT: find, find_one

DDL (Data Definition Language) operations:
- CREATE: create_collection, create_index
- DROP: drop_collection, drop_index
- LIST: list_collections, list_indexes
"""

import sys
import os

# Add the src directory to the path so we can import the library
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.core.connection import Connection, connect


def example_connection():
    """Example: How to connect to MongoDB"""
    print("\n" + "="*60)
    print("EXAMPLE 1: Connecting to MongoDB")
    print("="*60)
    
    # Method 1: Using connection string (DSN)
    print("\nMethod 1: Using connection string")
    try:
        conn = connect(
            dsn="mongodb://localhost:27017/testdb",
            connect_timeout=5.0,
            server_selection_timeout=5.0
        )
        print("✓ Connected using DSN")
        conn.close()
    except Exception as e:
        print(f"✗ Connection failed: {e}")
    
    # Method 2: Using individual parameters
    print("\nMethod 2: Using individual parameters")
    try:
        conn = connect(
            host="localhost",
            port=27017,
            database="testdb",
            connect_timeout=5.0,
            server_selection_timeout=5.0
        )
        print("✓ Connected using individual parameters")
        conn.close()
    except Exception as e:
        print(f"✗ Connection failed: {e}")
    
    # Method 3: Using context manager (recommended)
    print("\nMethod 3: Using context manager (recommended)")
    try:
        with connect(host="localhost", port=27017, database="testdb",
                     connect_timeout=5.0, server_selection_timeout=5.0) as conn:
            print("✓ Connected using context manager")
            print(f"  Database: {conn.database.name}")
            print(f"  Client: {conn.client}")
    except Exception as e:
        print(f"✗ Connection failed: {e}")


def example_ddl_operations():
    """Example: DDL operations (CREATE, DROP, LIST)"""
    print("\n" + "="*60)
    print("EXAMPLE 2: DDL Operations (Data Definition Language)")
    print("="*60)
    
    try:
        with connect(host="localhost", port=27017, database="testdb",
                     connect_timeout=5.0, server_selection_timeout=5.0) as conn:
            db = conn.database
            
            # List existing collections
            print("\n1. Listing existing collections:")
            existing_collections = db.list_collection_names()
            print(f"   Collections: {existing_collections}")
            
            # Create a collection (if it doesn't exist)
            print("\n2. Creating collection 'users':")
            if "users" not in existing_collections:
                users_collection = db.create_collection("users")
                print("   ✓ Collection 'users' created")
            else:
                users_collection = db["users"]
                print("   ✓ Collection 'users' already exists")
            
            # Create indexes
            print("\n3. Creating indexes:")
            # Single field index
            users_collection.create_index("email", unique=True)
            print("   ✓ Created unique index on 'email'")
            
            # Compound index
            users_collection.create_index([("name", 1), ("age", -1)])
            print("   ✓ Created compound index on 'name' (asc) and 'age' (desc)")
            
            # List indexes
            print("\n4. Listing indexes:")
            indexes = list(users_collection.list_indexes())
            for idx in indexes:
                print(f"   - {idx['name']}: {idx.get('key', {})}")
            
            # Drop an index
            print("\n5. Dropping an index:")
            try:
                users_collection.drop_index("name_1_age_-1")
                print("   ✓ Dropped compound index")
            except Exception as e:
                print(f"   Note: {e}")
            
            # Drop collection (commented out to keep data for DML examples)
            # print("\n6. Dropping collection:")
            # db.drop_collection("users")
            # print("   ✓ Collection 'users' dropped")
            
    except Exception as e:
        print(f"✗ DDL operations failed: {e}")


def example_dml_operations():
    """Example: DML operations (INSERT, UPDATE, DELETE, SELECT)"""
    print("\n" + "="*60)
    print("EXAMPLE 3: DML Operations (Data Manipulation Language)")
    print("="*60)
    
    try:
        with connect(host="localhost", port=27017, database="testdb",
                     connect_timeout=5.0, server_selection_timeout=5.0) as conn:
            collection = conn.collection("users")
            
            # INSERT operations
            print("\n1. INSERT Operations:")
            
            # Insert one document
            print("   a) Inserting one document:")
            result = collection.insert_one({
                "name": "Alice",
                "email": "alice@example.com",
                "age": 30,
                "city": "New York"
            })
            print(f"      ✓ Inserted document with _id: {result.inserted_id}")
            
            # Insert many documents
            print("   b) Inserting multiple documents:")
            documents = [
                {"name": "Bob", "email": "bob@example.com", "age": 25, "city": "Los Angeles"},
                {"name": "Charlie", "email": "charlie@example.com", "age": 35, "city": "Chicago"},
                {"name": "Diana", "email": "diana@example.com", "age": 28, "city": "New York"}
            ]
            result = collection.insert_many(documents)
            print(f"      ✓ Inserted {len(result.inserted_ids)} documents")
            
            # SELECT operations
            print("\n2. SELECT Operations:")
            
            # Find one document
            print("   a) Finding one document:")
            doc = collection.find_one({"name": "Alice"})
            if doc:
                print(f"      ✓ Found: {doc}")
            
            # Find many documents
            print("   b) Finding multiple documents:")
            cursor = collection.find({"age": {"$gt": 25}})
            docs = list(cursor)
            print(f"      ✓ Found {len(docs)} documents with age > 25")
            for doc in docs:
                print(f"        - {doc.get('name')} (age: {doc.get('age')})")
            
            # Count documents
            print("   c) Counting documents:")
            count = collection.count_documents({})
            print(f"      ✓ Total documents: {count}")
            
            # UPDATE operations
            print("\n3. UPDATE Operations:")
            
            # Update one document
            print("   a) Updating one document:")
            result = collection.update_one(
                {"name": "Alice"},
                {"$set": {"age": 31, "city": "Boston"}}
            )
            print(f"      ✓ Matched: {result.matched_count}, Modified: {result.modified_count}")
            
            # Update many documents
            print("   b) Updating multiple documents:")
            result = collection.update_many(
                {"city": "New York"},
                {"$inc": {"age": 1}}
            )
            print(f"      ✓ Matched: {result.matched_count}, Modified: {result.modified_count}")
            
            # Replace one document
            print("   c) Replacing one document:")
            result = collection.replace_one(
                {"name": "Bob"},
                {"name": "Robert", "email": "robert@example.com", "age": 26, "city": "San Francisco"}
            )
            print(f"      ✓ Matched: {result.matched_count}, Modified: {result.modified_count}")
            
            # DELETE operations
            print("\n4. DELETE Operations:")
            
            # Delete one document
            print("   a) Deleting one document:")
            result = collection.delete_one({"name": "Charlie"})
            print(f"      ✓ Deleted {result.deleted_count} document(s)")
            
            # Delete many documents
            print("   b) Deleting multiple documents:")
            result = collection.delete_many({"age": {"$lt": 30}})
            print(f"      ✓ Deleted {result.deleted_count} document(s)")
            
            # Final count
            print("\n5. Final document count:")
            count = collection.count_documents({})
            print(f"   ✓ Remaining documents: {count}")
            
    except Exception as e:
        print(f"✗ DML operations failed: {e}")


def example_advanced_queries():
    """Example: Advanced query operations"""
    print("\n" + "="*60)
    print("EXAMPLE 4: Advanced Query Operations")
    print("="*60)
    
    try:
        with connect(host="localhost", port=27017, database="testdb",
                     connect_timeout=5.0, server_selection_timeout=5.0) as conn:
            collection = conn.collection("users")
            
            # Insert sample data
            collection.insert_many([
                {"name": "Eve", "email": "eve@example.com", "age": 32, "city": "Seattle", "salary": 75000},
                {"name": "Frank", "email": "frank@example.com", "age": 29, "city": "Portland", "salary": 65000},
                {"name": "Grace", "email": "grace@example.com", "age": 27, "city": "Seattle", "salary": 80000}
            ])
            
            print("\n1. Query with multiple conditions:")
            cursor = collection.find({
                "$and": [
                    {"age": {"$gte": 25}},
                    {"age": {"$lte": 35}},
                    {"city": "Seattle"}
                ]
            })
            docs = list(cursor)
            print(f"   ✓ Found {len(docs)} documents")
            
            print("\n2. Sorting and limiting:")
            cursor = collection.find({}).sort("age", -1).limit(2)
            docs = list(cursor)
            print(f"   ✓ Top 2 oldest users:")
            for doc in docs:
                print(f"     - {doc.get('name')}: {doc.get('age')} years old")
            
            print("\n3. Projection (selecting specific fields):")
            cursor = collection.find({}, {"name": 1, "age": 1, "_id": 0})
            docs = list(cursor)
            print(f"   ✓ Selected fields only:")
            for doc in docs:
                print(f"     - {doc}")
            
    except Exception as e:
        print(f"✗ Advanced queries failed: {e}")


def main():
    """Run all examples"""
    print("\n" + "="*60)
    print("MongoDB Operations Example")
    print("="*60)
    print("\nThis example demonstrates:")
    print("  - Connection to MongoDB")
    print("  - DDL operations (CREATE, DROP, LIST)")
    print("  - DML operations (INSERT, UPDATE, DELETE, SELECT)")
    print("  - Advanced query operations")
    print("\nNote: Make sure MongoDB is running on localhost:27017")
    
    # Run examples
    example_connection()
    example_ddl_operations()
    example_dml_operations()
    example_advanced_queries()
    
    print("\n" + "="*60)
    print("Examples completed!")
    print("="*60)


if __name__ == "__main__":
    main()

