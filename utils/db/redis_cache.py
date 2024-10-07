import redis
import polars as pl
import numpy as np
import io
from typing import Optional, Tuple, List
import json
from datetime import datetime
from settings_archive import settings

# Configure logger
from utils.logging import get_logger

logger = get_logger('redis.log')


class RedisCache:
    def __init__(self):
        """
        Initializes the RedisCache with a Redis connection pool.
        """
        try:
            pool = redis.ConnectionPool(**settings.redis_cache_settings)
            self.redis = redis.Redis(connection_pool=pool)
            self.redis.ping()
            logger.info("Connected to Redis successfully.")
        except redis.RedisError as e:
            logger.critical(f"Failed to connect to Redis: {e}")
            raise

    def _get_metadata(self, df: pl.DataFrame) -> dict:
        """
        Extracts metadata from the DataFrame.
        """
        try:
            # Extract min and max datetimes as strings
            if 'datetime' in df.columns:
                min_datetime = df['datetime'].min()
                max_datetime = df['datetime'].max()

                # Convert Polars Datetime to string
                start_datetime = min_datetime.strftime("%Y-%m-%d %H:%M:%S")
                end_datetime = max_datetime.strftime("%Y-%m-%d %H:%M:%S")
            else:
                start_datetime = end_datetime = None

            return {
                "columns": df.columns,
                "start": start_datetime,
                "end": end_datetime,
                "num_rows": df.height
            }
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
            return {}

    def _store_df(self, key: str, df: pl.DataFrame, metadata: dict) -> None:
        """
        Stores the DataFrame and its metadata in Redis.
        """
        try:
            # Serialize DataFrame to Parquet
            buffer = io.BytesIO()
            df.write_parquet(buffer, compression="snappy")
            self.redis.set(key, buffer.getvalue())

            # Serialize metadata to JSON string
            metadata_json = json.dumps(metadata)
            self.redis.set(f"{key}:metadata", metadata_json)

            logger.info(f"Stored dataframe '{key}' with {df.height} rows.")
        except Exception as e:
            logger.error(f"Failed to store dataframe '{key}': {e}")

    def _get_df(self, key: str) -> Tuple[Optional[pl.DataFrame], Optional[dict]]:
        """
        Retrieves the DataFrame and its metadata from Redis.
        """
        try:
            data = self.redis.get(key)
            metadata_json = self.redis.get(f"{key}:metadata")

            if data and metadata_json:
                df = pl.read_parquet(io.BytesIO(data))
                metadata = json.loads(metadata_json.decode())
                return df, metadata
            else:
                return None, None
        except Exception as e:
            logger.error(f"Error reading dataframe '{key}': {e}")
            return None, None

    def get_metadata(self, key: str) -> Optional[dict]:
        """
        Retrieves metadata for a specific DataFrame.
        """
        _, metadata = self._get_df(key)

        if metadata:
            logger.info(f"Retrieved metadata for '{key}': {metadata}")
            return metadata
        else:
            logger.warning(f"No metadata found for '{key}'.")
            return None

    def upsert_or_get(self, key: str, df: Optional[pl.DataFrame] = None) -> Optional[pl.DataFrame]:
        """
        Inserts a new DataFrame or updates an existing one.
        - If df is None, retrieves the DataFrame stored under the key.
        - If df has the same schema as existing, concatenates new rows.
        - If df has a different schema, replaces the existing DataFrame.
        """
        existing_df, existing_metadata = self._get_df(key)

        if df is not None:
            new_metadata = self._get_metadata(df)

            if not new_metadata:
                logger.error("New DataFrame metadata extraction failed.")
                return None

            if existing_df is None:
                # No existing data, store the new dataframe
                self._store_df(key, df, new_metadata)
                logger.info(f"Stored new dataframe '{key}'.")
                return df
            else:
                # Check if the schemas match
                if set(df.columns) == set(existing_df.columns):
                    # Schemas match, concatenate and deduplicate
                    try:
                        merged_df = pl.concat([existing_df, df])
                        # Remove duplicates based on 'datetime', keeping the latest
                        if 'datetime' in merged_df.columns:
                            merged_df = merged_df.unique(subset=['datetime'], keep='last').sort('datetime')
                        else:
                            merged_df = merged_df.unique()

                        merged_metadata = self._get_metadata(merged_df)
                        self._store_df(key, merged_df, merged_metadata)
                        logger.info(f"Updated dataframe '{key}' with merged data.")
                        return merged_df
                    except Exception as e:
                        logger.error(f"Failed to merge and store dataframe '{key}': {e}")
                        return None
                else:
                    # Schemas differ, replace the existing dataframe
                    try:
                        self._store_df(key, df, new_metadata)
                        logger.info(f"Replaced dataframe '{key}' with new schema.")
                        return df
                    except Exception as e:
                        logger.error(f"Failed to replace dataframe '{key}': {e}")
                        return None
        else:
            # No new data provided, retrieve existing
            if existing_df is not None:
                logger.info(f"Retrieved existing dataframe '{key}'.")
                return existing_df
            else:
                logger.warning(f"No data found for '{key}'.")
                return None

    def delete(self, key: str) -> bool:
        """
        Deletes a specific DataFrame and its metadata from Redis based on the key.
        """
        try:
            # Delete both the DataFrame and its metadata
            result = self.redis.delete(key, f"{key}:metadata")
            if result >= 1:
                logger.info(f"Deleted dataframe and metadata for '{key}'.")
                return True
            else:
                logger.warning(f"No keys found to delete for '{key}'.")
                return False
        except Exception as e:
            logger.error(f"Error deleting keys for '{key}': {e}")
            return False

    def delete_all(self, pattern: str = "*") -> int:
        """
        Deletes all keys matching the given pattern from Redis.
        """
        try:
            keys = []
            cursor = 0
            while True:
                cursor, batch_keys = self.redis.scan(cursor=cursor, match=pattern, count=100)
                keys.extend(batch_keys)
                if cursor == 0:
                    break
            if keys:
                # Delete all matching keys
                result = self.redis.delete(*keys)
                logger.info(f"Deleted {result} keys matching pattern '{pattern}'.")
                return result
            else:
                logger.info(f"No keys found matching pattern '{pattern}'.")
                return 0
        except Exception as e:
            logger.error(f"Error deleting keys with pattern '{pattern}': {e}")
            return 0

    def flushdb(self) -> bool:
        """
        Flushes the entire Redis database. Use with caution as this will delete all data in the selected Redis database.
        """
        try:
            self.redis.flushdb()
            logger.info("Flushed the entire Redis database successfully.")
            return True
        except redis.RedisError as e:
            logger.error(f"Error flushing the Redis database: {e}")
            return False

    def list_keys(self, pattern: str = "*") -> List[str]:
        """
        Lists all keys stored in Redis for the specified pattern.
        """
        try:
            keys = []
            cursor = 0
            while True:
                cursor, batch_keys = self.redis.scan(cursor=cursor, match=pattern, count=100)
                keys.extend(batch_keys)
                if cursor == 0:
                    break
            # Decode bytes to strings
            keys = [key.decode('utf-8') for key in keys]
            logger.info(f"Retrieved {len(keys)} keys from Redis.")
            return keys
        except Exception as e:
            logger.error(f"Error listing keys: {e}")
            return []

    # --- New Methods to Encapsulate Usage Logic ---

    def store_initial_dataframes(self, tinkoff_df: pl.DataFrame, binance_df: pl.DataFrame) -> None:
        """
        Stores the initial DataFrames for Tinkoff and Binance.
        """
        # Define keys
        tinkoff_key = "tinkoff:sample_data:002"
        binance_key = "binance:sample_data:003"

        # Upsert DataFrames
        self.upsert_or_get(tinkoff_key, tinkoff_df)
        self.upsert_or_get(binance_key, binance_df)
        logger.info("Stored initial DataFrames for Tinkoff and Binance.")

    def add_new_tinkoff_data(self, new_tinkoff_df: pl.DataFrame) -> Optional[pl.DataFrame]:
        """
        Adds new data with a new datetime range for Tinkoff, concatenating with existing DataFrame.
        """
        tinkoff_key = "tinkoff:sample_data:002"
        updated_df = self.upsert_or_get(tinkoff_key, new_tinkoff_df)
        if updated_df is not None:
            logger.info(f"Added new Tinkoff data to '{tinkoff_key}'.")
        else:
            logger.error(f"Failed to add new Tinkoff data to '{tinkoff_key}'.")
        return updated_df

    def add_new_binance_columns(self, new_binance_df_with_new_columns: pl.DataFrame) -> Optional[pl.DataFrame]:
        """
        Adds new columns to Binance DataFrame, replacing the existing DataFrame if schema changes.
        """
        binance_key = "binance:sample_data:003"
        updated_df = self.upsert_or_get(binance_key, new_binance_df_with_new_columns)
        if updated_df is not None:
            logger.info(f"Added new columns to Binance DataFrame with key '{binance_key}'.")
        else:
            logger.error(f"Failed to add new columns to Binance DataFrame with key '{binance_key}'.")
        return updated_df


def main():
    # Initialize the RedisCache
    r_cache = RedisCache()

    # --- Phase 1: Storing Initial DataFrames for Tinkoff and Binance ---
    print("\n--- Phase 1: Storing Initial DataFrames for Tinkoff and Binance ---\n")

    # Define start and end datetime for Tinkoff
    tinkoff_start = datetime.strptime('2024-02-01 09:30:00', '%Y-%m-%d %H:%M:%S')
    tinkoff_end = datetime.strptime('2024-02-01 09:59:00', '%Y-%m-%d %H:%M:%S')

    # Generate initial sample data for Tinkoff with 1-minute frequency
    tinkoff_df_initial = pl.DataFrame({
        'datetime': pl.datetime_range(tinkoff_start, tinkoff_end, interval='1m', eager=True),
        'price': np.random.rand(30) * 200 + 100,
        'volume': np.random.randint(500, 5000, size=30)
    })

    # Define start and end datetime for Binance
    binance_start = datetime.strptime('2024-03-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    binance_end = datetime.strptime('2024-03-01 00:39:00', '%Y-%m-%d %H:%M:%S')

    # Generate initial sample data for Binance with 1-minute frequency
    binance_df_initial = pl.DataFrame({
        'datetime': pl.datetime_range(binance_start, binance_end, interval='1m', eager=True),
        'price': np.random.rand(40) * 300 + 150,
        'volume': np.random.randint(2000, 20000, size=40)
    })

    # Store initial DataFrames
    r_cache.store_initial_dataframes(tinkoff_df_initial, binance_df_initial)
    print("Initial DataFrames for Tinkoff and Binance have been stored.\n")

    # --- Phase 2: Adding New Data with New Datetime Range for Tinkoff ---
    print("\n--- Phase 2: Adding New Data with New Datetime Range for Tinkoff ---\n")

    # Define new start and end datetime for the new Tinkoff data
    tinkoff_new_start = datetime.strptime('2024-02-01 10:00:00', '%Y-%m-%d %H:%M:%S')
    tinkoff_new_end = datetime.strptime('2024-02-01 10:29:00', '%Y-%m-%d %H:%M:%S')  # 30 minutes

    # Generate new sample data for Tinkoff with a new 1-minute frequency
    tinkoff_df_new = pl.DataFrame({
        'datetime': pl.datetime_range(tinkoff_new_start, tinkoff_new_end, interval='1m', eager=True),
        'price': np.random.rand(30) * 200 + 100,
        'volume': np.random.randint(500, 5000, size=30)
    })

    # Add new Tinkoff data (concatenation)
    updated_tinkoff_df = r_cache.add_new_tinkoff_data(tinkoff_df_new)
    if updated_tinkoff_df is not None:
        print(f"Added new data to Tinkoff DataFrame:\n{updated_tinkoff_df}\n")
    else:
        print("Failed to add new data to Tinkoff DataFrame.\n")

    # --- Phase 3: Adding New Columns for Binance ---
    print("\n--- Phase 3: Adding New Columns for Binance ---\n")

    # Generate updated Binance DataFrame with new columns 'bid' and 'ask'
    binance_df_updated = pl.DataFrame({
        'datetime': binance_df_initial['datetime'],  # Reuse existing datetime
        'price': binance_df_initial['price'],
        'volume': binance_df_initial['volume'],
        'bid': np.random.rand(40) * 300 + 150,
        'ask': np.random.rand(40) * 300 + 150
    })

    # Add new columns to Binance DataFrame (replacement)
    updated_binance_df = r_cache.add_new_binance_columns(binance_df_updated)
    if updated_binance_df is not None:
        print(f"Added new columns to Binance DataFrame:\n{updated_binance_df}\n")
    else:
        print("Failed to add new columns to Binance DataFrame.\n")

    # --- Phase 4: Retrieving and Displaying Updated DataFrames ---
    print("\n--- Phase 4: Retrieving and Displaying Updated DataFrames ---\n")

    # Retrieve updated Tinkoff DataFrame
    tinkoff_key = "tinkoff:sample_data:002"
    retrieved_tinkoff_df = r_cache.upsert_or_get(tinkoff_key)
    if retrieved_tinkoff_df is not None:
        print(f"Retrieved updated Tinkoff DataFrame with key '{tinkoff_key}':\n{retrieved_tinkoff_df}\n")
    else:
        print(f"Failed to retrieve Tinkoff DataFrame with key '{tinkoff_key}'.\n")

    # Retrieve updated Binance DataFrame
    binance_key = "binance:sample_data:003"
    retrieved_binance_df = r_cache.upsert_or_get(binance_key)
    if retrieved_binance_df is not None:
        print(f"Retrieved updated Binance DataFrame with key '{binance_key}':\n{retrieved_binance_df}\n")
    else:
        print(f"Failed to retrieve Binance DataFrame with key '{binance_key}'.\n")

    # --- Phase 5: Retrieving Metadata ---
    print("\n--- Phase 5: Retrieving Metadata ---\n")

    # Retrieve metadata for Tinkoff
    tinkoff_metadata = r_cache.get_metadata(tinkoff_key)
    if tinkoff_metadata:
        print(f"Metadata for '{tinkoff_key}': {tinkoff_metadata}\n")
    else:
        print(f"No metadata found for '{tinkoff_key}'.\n")

    # Retrieve metadata for Binance
    binance_metadata = r_cache.get_metadata(binance_key)
    if binance_metadata:
        print(f"Metadata for '{binance_key}': {binance_metadata}\n")
    else:
        print(f"No metadata found for '{binance_key}'.\n")

    # --- Phase 6: Listing All Keys in Redis ---
    print("\n--- Phase 6: Listing All Keys in Redis ---\n")
    all_keys = r_cache.list_keys()
    print(f"All keys in Redis:\n{all_keys}\n")

    # --- Phase 7: (Optional) Deleting Specific Keys or Flushing Database ---
    # Uncomment the sections below to perform deletions as needed.

    # --- Deleting a Specific Key ---
    # Example: Delete by key
    # delete_key = "tinkoff:sample_data:002"
    # delete_success = r_cache.delete(delete_key)
    # if delete_success:
    #     print(f"Successfully deleted the DataFrame and metadata for key '{delete_key}'.\n")
    # else:
    #     print(f"Failed to delete the DataFrame and metadata for key '{delete_key}'.\n")

    # --- Deleting All Data for a Specific Exchange ---
    # Example: Delete all Binance data
    # delete_pattern = "binance:*"
    # deleted_keys_count = r_cache.delete_all(pattern=delete_pattern)
    # print(f"Number of keys deleted matching pattern '{delete_pattern}': {deleted_keys_count}\n")

    # --- Deleting All Data in Redis ---
    # Flush the entire Redis database (use with caution)
    # flush_success = r_cache.flushdb()
    # if flush_success:
    #     print("Successfully flushed the entire Redis database.\n")
    # else:
    #     print("Failed to flush the Redis database.\n")

    # --- Listing Keys After Deletions ---
    # keys_after_deletion = r_cache.list_keys()
    # print(f"All keys in Redis after deletions:\n{keys_after_deletion}\n")


if __name__ == '__main__':
    main()