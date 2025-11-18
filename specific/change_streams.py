"""MongoDB Change Streams Support"""

import threading
from typing import Callable, Optional

from ..core.connection import Connection
from ..core.exceptions import OperationalError


class ChangeStreamListener:
    """Listener for MongoDB change stream events"""

    def __init__(self, connection: Connection):
        self._conn = connection
        self._listening = False
        self._collection: str | None = None
        self._database: str | None = None
        self._pipeline: list[dict] | None = None
        self._callback: Optional[Callable] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._change_stream = None

    def watch(
            self,
            collection: str | None = None,
            database: str | None = None,
            pipeline: list[dict] | None = None,
            callback: Optional[Callable] = None
    ) -> None:
        """
        Watch for changes.
        
        Args:
            collection: Collection name to watch (None for database-level)
            database: Database name to watch (None for cluster-level)
            pipeline: Aggregation pipeline to filter changes
            callback: Callback function(change_document)
        """
        self._collection = collection
        self._database = database
        self._pipeline = pipeline
        self._callback = callback

    def start(self) -> None:
        """Start the change stream listener"""
        if self._listening:
            return
        
        if not self._conn or self._conn.closed:
            raise OperationalError("Connection is closed")
        
        try:
            # Get the target (collection, database, or client)
            if self._collection:
                target = self._conn.collection(self._collection)
            elif self._database:
                target = self._conn.client[self._database]
            else:
                target = self._conn.client
            
            # Create change stream
            if self._pipeline:
                self._change_stream = target.watch(self._pipeline)
            else:
                self._change_stream = target.watch()
            
            self._listening = True
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._listen_loop, daemon=True)
            self._thread.start()
        except Exception as e:
            raise OperationalError(f"Failed to start change stream: {e}")

    def stop(self) -> None:
        """Stop the change stream listener"""
        if not self._listening:
            return
        
        self._listening = False
        self._stop_event.set()
        
        if self._change_stream:
            try:
                self._change_stream.close()
            except:
                pass
            self._change_stream = None
        
        if self._thread:
            self._thread.join(timeout=1.0)

    def _listen_loop(self) -> None:
        """Main listening loop"""
        try:
            while self._listening and not self._stop_event.is_set():
                try:
                    if self._change_stream:
                        # Get next change (with timeout)
                        change = self._change_stream.next()
                        if change and self._callback:
                            self._callback(change)
                except StopIteration:
                    # Change stream ended
                    break
                except Exception as e:
                    if self._listening:
                        # Log error but continue
                        print(f"Change stream error: {e}")
        finally:
            self._listening = False

    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()


def create_listener(
        connection: Connection,
        collection: str | None = None,
        database: str | None = None,
        pipeline: list[dict] | None = None,
        callback: Optional[Callable] = None
) -> ChangeStreamListener:
    """Create a change stream listener"""
    listener = ChangeStreamListener(connection)
    listener.watch(collection, database, pipeline, callback)
    return listener

