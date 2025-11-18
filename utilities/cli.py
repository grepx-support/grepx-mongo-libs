"""Command-Line Interface for MongoDB"""

import argparse
import json
from pprint import pprint

from core.connection import connect


class MongoDBCLI:
    """Interactive MongoDB command-line interface"""

    def __init__(self, dsn: str | None = None, **kwargs):
        self.dsn = dsn
        self.connection_kwargs = kwargs
        self.connection = None
        self.prompt = "mongodb> "
        self.continuation_prompt = "   ...> "

    def connect(self) -> None:
        """Connect to database"""
        if self.dsn:
            self.connection = connect(dsn=self.dsn, **self.connection_kwargs)
        else:
            self.connection = connect(**self.connection_kwargs)

        # Get database name for display
        db_name = self.connection_kwargs.get('database', 'test')
        print(f"Connected to MongoDB database: {db_name}")

    def execute_file(self, filepath: str) -> None:
        """Execute MongoDB operations from JSON file"""
        if not self.connection:
            print("Not connected to database")
            return

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Execute operations
            if isinstance(data, list):
                for op in data:
                    self.execute_operation(op)
            elif isinstance(data, dict):
                self.execute_operation(data)

            print(f"Executed script from {filepath}")
        except Exception as e:
            print(f"Error executing script: {e}")

    def execute_operation(self, operation: dict) -> None:
        """Execute a MongoDB operation"""
        if not self.connection:
            print("Not connected to database")
            return

        try:
            op_type = operation.get("type")
            collection = operation.get("collection")

            if not collection:
                print("Error: Collection not specified")
                return

            coll = self.connection.collection(collection)

            if op_type == "find":
                filter = operation.get("filter", {})
                projection = operation.get("projection")
                limit = operation.get("limit")
                skip = operation.get("skip")
                sort = operation.get("sort")

                cursor = coll.find(filter, projection)
                if sort:
                    cursor = cursor.sort(sort)
                if skip:
                    cursor = cursor.skip(skip)
                if limit:
                    cursor = cursor.limit(limit)

                results = list(cursor)
                self._print_results(results)

            elif op_type == "find_one":
                filter = operation.get("filter", {})
                projection = operation.get("projection")
                result = coll.find_one(filter, projection)
                if result:
                    self._print_results([result])
                else:
                    print("No document found")

            elif op_type == "insert_one":
                document = operation.get("document", {})
                result = coll.insert_one(document)
                print(f"Inserted document with _id: {result.inserted_id}")

            elif op_type == "insert_many":
                documents = operation.get("documents", [])
                result = coll.insert_many(documents)
                print(f"Inserted {len(result.inserted_ids)} documents")

            elif op_type == "update_one":
                filter = operation.get("filter", {})
                update = operation.get("update", {})
                upsert = operation.get("upsert", False)
                result = coll.update_one(filter, update, upsert=upsert)
                print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")

            elif op_type == "update_many":
                filter = operation.get("filter", {})
                update = operation.get("update", {})
                upsert = operation.get("upsert", False)
                result = coll.update_many(filter, update, upsert=upsert)
                print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")

            elif op_type == "delete_one":
                filter = operation.get("filter", {})
                result = coll.delete_one(filter)
                print(f"Deleted {result.deleted_count} document(s)")

            elif op_type == "delete_many":
                filter = operation.get("filter", {})
                result = coll.delete_many(filter)
                print(f"Deleted {result.deleted_count} document(s)")

            elif op_type == "aggregate":
                pipeline = operation.get("pipeline", [])
                cursor = coll.aggregate(pipeline)
                results = list(cursor)
                self._print_results(results)

            elif op_type == "count":
                filter = operation.get("filter", {})
                count = coll.count_documents(filter)
                print(f"Count: {count}")

            else:
                print(f"Unknown operation type: {op_type}")

        except Exception as e:
            print(f"Error: {e}")

    def _print_results(self, results: list) -> None:
        """Print query results"""
        if not results:
            print("No results")
            return

        for doc in results:
            pprint(doc)

    def run_interactive(self) -> None:
        """Run interactive REPL"""
        self.connect()

        print("MongoDB Interactive Shell")
        print("Enter '.help' for help, '.quit' to exit")

        buffer = []

        try:
            while True:
                try:
                    if buffer:
                        line = input(self.continuation_prompt)
                    else:
                        line = input(self.prompt)

                    line = line.strip()

                    # Handle dot commands
                    if line.startswith('.'):
                        if self.handle_command(line):
                            break
                        continue

                    # Accumulate JSON operation
                    buffer.append(line)
                    text = ' '.join(buffer)

                    # Try to parse as JSON
                    try:
                        operation = json.loads(text)
                        self.execute_operation(operation)
                        buffer = []
                    except json.JSONDecodeError:
                        # Not complete JSON yet, continue
                        pass

                except EOFError:
                    print("\nExiting...")
                    break
                except KeyboardInterrupt:
                    print("\nInterrupted. Type '.quit' to exit.")
                    buffer = []
        finally:
            if self.connection:
                self.connection.close()

    def handle_command(self, command: str) -> bool:
        """Handle dot commands"""
        parts = command.split()
        cmd = parts[0]

        if cmd == '.quit' or cmd == '.exit':
            return True
        elif cmd == '.help':
            print("""
Commands:
  .help          Show this help message
  .quit, .exit   Exit the shell
  .collections   List all collections
  .use <db>      Switch database
  .show <coll>   Show documents from collection
  
Operations are specified as JSON:
  {"type": "find", "collection": "users", "filter": {"age": {"$gt": 18}}}
            """)
        elif cmd == '.collections':
            if self.connection:
                collections = self.connection.database.list_collection_names()
                for coll in collections:
                    print(f"  {coll}")
            else:
                print("Not connected")
        elif cmd == '.use' and len(parts) > 1:
            db_name = parts[1]
            self.connection_kwargs['database'] = db_name
            self.connection.close()
            self.connect()
        elif cmd == '.show' and len(parts) > 1:
            coll_name = parts[1]
            operation = {
                "type": "find",
                "collection": coll_name,
                "limit": 10
            }
            self.execute_operation(operation)
        else:
            print(f"Unknown command: {cmd}. Type '.help' for help.")

        return False


def main():
    """Main entry point for CLI"""
    parser = argparse.ArgumentParser(description="MongoDB Interactive Shell")
    parser.add_argument("dsn", nargs="?", help="MongoDB connection URI")
    parser.add_argument("--host", help="MongoDB host")
    parser.add_argument("--port", type=int, help="MongoDB port")
    parser.add_argument("--database", help="Database name")
    parser.add_argument("--username", help="Username")
    parser.add_argument("--password", help="Password")
    parser.add_argument("--file", help="Execute operations from JSON file")

    args = parser.parse_args()

    kwargs = {}
    if args.host:
        kwargs['host'] = args.host
    if args.port:
        kwargs['port'] = args.port
    if args.database:
        kwargs['database'] = args.database
    if args.username:
        kwargs['username'] = args.username
    if args.password:
        kwargs['password'] = args.password

    cli = MongoDBCLI(dsn=args.dsn, **kwargs)

    if args.file:
        cli.connect()
        cli.execute_file(args.file)
        cli.connection.close()
    else:
        cli.run_interactive()


if __name__ == "__main__":
    main()
