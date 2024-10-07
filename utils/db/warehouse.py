from pathlib import Path
from typing import Optional, List, Union, Dict, Any
import polars as pl
from dataclasses import dataclass
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from loguru import logger

# Remove the direct import to avoid circular dependencies
# from settings import get_settings

@dataclass
class ParquetConfig:
    compression: str = 'zstd'
    compression_level: int = 3
    row_group_size: int = 100_000
    use_pyarrow: bool = True
    statistics: bool = True


class WareHouse:
    def __init__(self, settings, config: Optional[ParquetConfig] = None):
        self.settings = settings  # Store the Settings instance
        logger.debug(f"Settings: {self.settings}")
        logger.debug(f"Settings: {self.settings.paths}")
        self.base_path = Path(self.settings.paths['parquet_path']).resolve()
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.config = config or ParquetConfig()

    def _resolve_path(self, filename: Union[str, Path]) -> Path:
        try:
            filepath = (self.base_path / Path(filename)).resolve()
            if not str(filepath).startswith(str(self.base_path)):
                raise ValueError(f"Invalid filename '{filename}': Path traversal detected.")
            filepath.parent.mkdir(parents=True, exist_ok=True)
            return filepath
        except Exception as e:
            logger.error(f"Error resolving path: {e}")
            raise

    def save_parquet(
            self,
            df: Union[pl.DataFrame, pl.LazyFrame],
            filename: Union[str, Path],
            partition_cols: Optional[List[str]] = None
    ) -> bool:
        """
        Save data to Parquet format with optimizations.
        """
        try:
            filepath = self._resolve_path(filename)
            logger.info(f"Saving data to {filepath}")

            if isinstance(df, pl.LazyFrame):
                df = df.collect()

            if self.config.use_pyarrow:
                if partition_cols:
                    for partition_val in df.select(partition_cols).unique().iter_rows():
                        partition_df = df.filter(pl.col(partition_cols[0]) == partition_val[0])

                        partition_dir = filepath.parent / f"{partition_cols[0]}={partition_val[0]}"
                        partition_dir.mkdir(parents=True, exist_ok=True)
                        partition_filepath = partition_dir / filepath.name

                        pa_table = partition_df.to_arrow()
                        pq.write_table(
                            pa_table,
                            str(partition_filepath),
                            compression=self.config.compression,
                            compression_level=self.config.compression_level,
                            row_group_size=self.config.row_group_size,
                            write_statistics=self.config.statistics
                        )
                else:
                    pa_table = df.to_arrow()
                    pq.write_table(
                        pa_table,
                        str(filepath),
                        compression=self.config.compression,
                        compression_level=self.config.compression_level,
                        row_group_size=self.config.row_group_size,
                        write_statistics=self.config.statistics
                    )
            else:
                df.write_parquet(
                    str(filepath),
                    compression=self.config.compression,
                    statistics=self.config.statistics
                )

            logger.success(f"Successfully saved data to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error saving Parquet file: {e}")
            return False

    def read_parquet(
            self,
            filename: Union[str, Path],
            columns: Optional[List[str]] = None,
            filters: Optional[List[tuple]] = None,
            lazy: bool = True,
            row_count: Optional[int] = None,
    ) -> Optional[Union[pl.DataFrame, pl.LazyFrame]]:
        """
        Read Parquet file with optimizations.
        """
        try:
            filepath = self._resolve_path(filename)

            if not filepath.exists():
                logger.error(f"File not found: {filepath}")
                return None

            logger.info(f"Reading data from {filepath}")

            read_options = {
                "columns": columns,
            }

            if filters:
                logger.debug(f"Applying filters: {filters}")

            if row_count:
                read_options["n_rows"] = row_count
                logger.debug(f"Limiting to {row_count} rows")

            if lazy:
                return pl.scan_parquet(str(filepath), **read_options)
            else:
                return pl.read_parquet(str(filepath), **read_options)

        except Exception as e:
            logger.error(f"Error reading Parquet file: {e}")
            return None

    def get_metadata(self, filename: Union[str, Path]) -> Optional[Dict[str, Any]]:
        """Get detailed metadata about a Parquet file."""
        try:
            filepath = self._resolve_path(filename)
            logger.info(f"Getting metadata for {filepath}")

            if self.config.use_pyarrow:
                parquet_file = pq.ParquetFile(filepath)
                metadata = parquet_file.metadata

                return {
                    'num_rows': metadata.num_rows,
                    'num_row_groups': metadata.num_row_groups,
                    'schema': metadata.schema.to_arrow_schema().to_string(),
                    'columns': [metadata.row_group(0).column(i).statistics for i in range(metadata.num_columns)],
                    'total_byte_size': metadata.serialized_size,
                    'compression': metadata.row_group(0).column(0).compression,
                }
            else:
                schema = pl.read_parquet_schema(str(filepath))
                stats = filepath.stat()

                return {
                    'columns': list(schema.items()),
                    'dtypes': {col: str(dtype) for col, dtype in schema.items()},
                    'size_bytes': stats.st_size,
                }

        except Exception as e:
            logger.error(f"Error reading metadata: {e}")
            return None


def test_warehouse():
    # Get the warehouse instance from settings
    settings = get_settings("tinkoff")  # Or any other exchange
    warehouse = settings.get_warehouse()

    try:
        # Create sample data
        start_date_str = "2022-01-01 10:00:00"
        end_date_str = "2022-01-01 11:00:00"
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S")

        date_range = pl.datetime_range(start_date, end_date, interval="1m", eager=True)

        data = pl.DataFrame({
            'symbol': ['BTC'] * len(date_range),
            'datetime': date_range,
            'price': [100.0 + i * 0.01 for i in range(len(date_range))],  # Dummy price data
            'volume': [1000.0 + i for i in range(len(date_range))]  # Dummy volume data
        })

        # Test saving with partitioning
        success = warehouse.save_parquet(
            data,
            'crypto/prices.parquet',
            partition_cols=['symbol']
        )

        # Test reading with optimizations
        df = warehouse.read_parquet(
            'crypto/prices.parquet',
            columns=['date', 'price'],
            filters=[('price', '>', 28100)],
            lazy=False
        )
        if df is not None:
            logger.info(f"Read {len(df)} rows")
            logger.debug(f"First few rows:\n{df.head()}")

        # Test metadata
        metadata = warehouse.get_metadata('crypto/prices.parquet')
        if metadata:
            logger.info("Parquet Metadata:")
            for key, value in metadata.items():
                logger.debug(f"{key}: {value}")

    except Exception as e:
        logger.exception(f"Error during warehouse test: {e}")


if __name__ == "__main__":
    settings_tinkoff = get_settings("tinkoff")
    logger = settings_tinkoff.get_logger()
    test_warehouse()