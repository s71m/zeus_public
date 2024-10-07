import sys
import time
import threading
from queue import Queue, Empty
from typing import Optional, Dict, Any

from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError, OperationalError
import polars as pl
import pyarrow as pa
from loguru import logger


class ClickHousePool:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(ClickHousePool, cls).__new__(cls)
        return cls._instance

    def __init__(self, app, pool_size: int = 10, recycle: int = 3600):
        if hasattr(self, '_initialized'):
            return

        self.app = app
        self.pool_size = pool_size
        self.recycle = recycle

        self._pool: Queue = Queue(maxsize=pool_size)
        self._used_connections: Dict[Client, float] = {}
        self._initialized = True

        # Configure logging based on settings
        # logger = self.app.get_logger()

        # Get debug settings from global config
        self.debug_log = self.app.cfg.get('clickhouse_debug_log', False)
        self.query_log = self.app.cfg.get('clickhouse_query_log', False)

        try:
            self._create_connection()
            if self.debug_log:
                logger.debug("ClickHousePool initialized successfully")
        except OperationalError as e:
            logger.critical(f"Failed to initialize ClickHousePool: {str(e)}")
            raise SystemExit(1)  # Exit the application

    def _create_connection(self) -> Client:
        clickhouse_settings = self.app.env_config.get('clickhouse_settings', {})
        connection = get_client(**clickhouse_settings)
        if self.debug_log:
            logger.debug(f"Created new connection: {id(connection)}")
        return connection

    def _get_connection(self) -> Client:
        try:
            connection = self._pool.get(block=False)
            with self._lock:
                if connection in self._used_connections:
                    last_used = self._used_connections[connection]
                    if time.time() - last_used > self.recycle:
                        if self.debug_log:
                            logger.info(f"Recycling connection: {id(connection)}")
                        try:
                            connection.disconnect()
                        except Exception as e:
                            logger.error(f"Error disconnecting connection {id(connection)}: {e}")
                        connection = self._create_connection()
                self._used_connections[connection] = time.time()
            if self.debug_log:
                logger.debug(f"Got connection from pool: {id(connection)}")
            return connection
        except Empty:
            if self._pool.qsize() + len(self._used_connections) < self.pool_size:
                connection = self._create_connection()
                with self._lock:
                    self._used_connections[connection] = time.time()
                if self.debug_log:
                    logger.debug(f"Created new connection as pool was empty: {id(connection)}")
                return connection
            logger.error("No available connections in the pool")
            raise ClickHouseError("No available connections in the pool")

    def _return_connection(self, connection: Client):
        with self._lock:
            if connection in self._used_connections:
                del self._used_connections[connection]
        self._pool.put(connection)
        if self.debug_log:
            logger.debug(f"Returned connection to pool: {id(connection)}")

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None):
        connection = self._get_connection()
        try:
            result = connection.command(query, parameters=params)
            if self.query_log:
                logger.info(f"Executing query: {query}")
            return result
        except ClickHouseError as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            self._return_connection(connection)

    def query(self, query: str, params: Optional[Dict[str, Any]] = None):
        connection = self._get_connection()
        try:
            result = connection.query(query, parameters=params)
            if self.query_log:
                logger.info(f"Executing query: {query}")
            return result
        except ClickHouseError as e:
            logger.error(f"Error querying data: {e}")
            raise
        finally:
            self._return_connection(connection)

    def query_df(self, query: str, params: Optional[Dict[str, Any]] = None):
        connection = self._get_connection()
        try:
            result = connection.query_df(query, parameters=params)
            if self.query_log:
                logger.info(f"Executing query: {query}")
            return result
        except ClickHouseError as e:
            logger.error(f"Error querying DataFrame: {e}")
            raise
        finally:
            self._return_connection(connection)

    def query_arrow(self, query: str, params: Optional[Dict[str, Any]] = None) -> pa.Table:
        connection = self._get_connection()
        try:
            result = connection.query_arrow(query, parameters=params)
            if self.query_log:
                logger.info(f"Executing query: {query}")
            return result
        except ClickHouseError as e:
            logger.error(f"Error querying Arrow table: {e}")
            raise
        finally:
            self._return_connection(connection)

    def query_polars(self, query: str, params: Optional[Dict[str, Any]] = None) -> pl.DataFrame:
        try:
            arrow_table = self.query_arrow(query, params)
            return pl.from_arrow(arrow_table)
        except ClickHouseError as e:
            logger.error(f"Error converting Arrow to Polars DataFrame: {e}")
            raise

    def insert_polars(self, table: str, df: pl.DataFrame):
        connection = self._get_connection()
        try:
            arrow_table = df.to_arrow()
            connection.insert_arrow(table, arrow_table)
            if self.debug_log:
                logger.debug(f"Inserted Polars DataFrame into table: {table}")
        except ClickHouseError as e:
            logger.error(f"Error inserting Polars DataFrame into table {table}: {e}")
            raise
        finally:
            self._return_connection(connection)

    def close(self):
        while not self._pool.empty():
            connection = self._pool.get()
            try:
                connection.disconnect()
                if self.debug_log:
                    logger.debug(f"Disconnected connection: {id(connection)}")
            except Exception as e:
                logger.error(f"Error disconnecting connection {id(connection)}: {e}")

        with self._lock:
            for connection in list(self._used_connections.keys()):
                try:
                    connection.close()
                    if self.debug_log:
                        logger.debug(f"Disconnected connection: {id(connection)}")
                except Exception as e:
                    logger.error(f"Error disconnecting connection {id(connection)}: {e}")
            self._used_connections.clear()
        if self.debug_log:
            logger.debug("ClickHousePool closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()