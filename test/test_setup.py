"""
Test script to verify the MongoDB library setup works correctly.
This script performs basic connectivity and functionality tests.
"""

import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.core.connection import Connection, connect
from src.core.exceptions import InterfaceError


def test_imports():
    """Test that all required modules can be imported"""
    print("Testing imports...")
    try:
        from src.core.connection import Connection, connect
        from src.core.constants import DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DATABASE
        from src.core.exceptions import InterfaceError
        print("✓ All imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False


def test_connection_creation():
    """Test that connection objects can be created"""
    print("\nTesting connection object creation...")
    try:
        conn = Connection(
            host="localhost",
            port=27017,
            database="testdb"
        )
        print("✓ Connection object created successfully")
        print(f"  - Host: {conn._host}")
        print(f"  - Port: {conn._port}")
        print(f"  - Database: {conn._database}")
        return True
    except Exception as e:
        print(f"✗ Connection creation failed: {e}")
        return False


def test_connection_connectivity():
    """Test actual connection to MongoDB server"""
    print("\nTesting MongoDB connectivity...")
    print("  (This requires MongoDB to be running on localhost:27017)")
    
    try:
        conn = connect(
            host="localhost",
            port=27017,
            database="testdb",
            connect_timeout=5.0,
            server_selection_timeout=5.0
        )
        
        # Test database access
        db = conn.database
        print(f"✓ Connected to MongoDB")
        print(f"  - Database name: {db.name}")
        
        # Test client access
        client = conn.client
        print(f"  - Client: {type(client).__name__}")
        
        # Test server info
        try:
            server_info = client.server_info()
            print(f"  - MongoDB version: {server_info.get('version', 'unknown')}")
        except Exception:
            print("  - Could not retrieve server info")
        
        conn.close()
        print("✓ Connection closed successfully")
        return True
        
    except InterfaceError as e:
        print(f"✗ Connection failed: {e}")
        print("  Make sure MongoDB is running and accessible")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False


def test_collection_operations():
    """Test basic collection operations"""
    print("\nTesting collection operations...")
    
    try:
        with connect(host="localhost", port=27017, database="testdb",
                     connect_timeout=5.0, server_selection_timeout=5.0) as conn:
            
            # Get collection
            collection = conn.collection("test_setup")
            print("✓ Collection object retrieved")
            
            # Insert a test document
            result = collection.insert_one({"test": "setup", "value": 123})
            print(f"✓ Document inserted: {result.inserted_id}")
            
            # Find the document
            doc = collection.find_one({"test": "setup"})
            if doc:
                print(f"✓ Document found: {doc}")
            
            # Delete the test document
            result = collection.delete_one({"test": "setup"})
            print(f"✓ Test document deleted: {result.deleted_count} document(s)")
            
            return True
            
    except Exception as e:
        print(f"✗ Collection operations failed: {e}")
        return False


def test_context_manager():
    """Test connection context manager"""
    print("\nTesting context manager...")
    
    try:
        with connect(host="localhost", port=27017, database="testdb",
                     connect_timeout=5.0, server_selection_timeout=5.0) as conn:
            print("✓ Context manager entered successfully")
            print(f"  - Connection closed: {conn.closed}")
        
        # After exiting context, connection should be closed
        print("✓ Context manager exited successfully")
        return True
        
    except Exception as e:
        print(f"✗ Context manager test failed: {e}")
        return False


def run_all_tests():
    """Run all tests"""
    print("="*60)
    print("MongoDB Library Setup Test")
    print("="*60)
    
    tests = [
        ("Imports", test_imports),
        ("Connection Creation", test_connection_creation),
        ("MongoDB Connectivity", test_connection_connectivity),
        ("Collection Operations", test_collection_operations),
        ("Context Manager", test_context_manager),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n✗ {test_name} raised exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n✓ All tests passed! Setup is working correctly.")
        return 0
    else:
        print(f"\n✗ {total - passed} test(s) failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)

