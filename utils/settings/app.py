from pathlib import Path
from typing import Dict, Optional, Any
from loguru import logger

from .logger_config import setup_logger
from .exchange_settings import ExchangeConfig, ExchangeProxy


class App:
    def __init__(
            self,
            app_name: str,
            global_config: Dict[str, Any],
            env_config: Dict[str, Any],
            app_config: Dict[str, Any],
            exchanges_config: Dict[str, Any],
            exchange_paths: Dict[str, str]
    ):

        self.cfg = {
            'app_name': app_name,
            **global_config.get('global', {}),  # Merge global settings first
            **env_config,  # Then environment settings
            **app_config  # Finally app-specific settings
        }

        # Store references instead of copying
        self.global_config = global_config
        self.env_config = env_config
        self.app_config = app_config
        self.exchanges_config = exchanges_config
        self.exchange_paths = exchange_paths

        self._exchange_config: Optional[ExchangeConfig] = None
        self._loggers: Dict[str, Any] = {}
        self.exchange = ExchangeProxy(self)
        self._default_logger = self.get_logger()

        self._warehouse = None

    def set_exchange(self, exchange_name: str) -> None:
        """
        Sets the exchange configuration for the app.

        Parameters:
            exchange_name (str): The name of the exchange to set.

        Raises:
            ValueError: If the specified exchange does not exist in the configuration.
        """
        if exchange_name not in self.exchanges_config:
            raise ValueError(f"Exchange '{exchange_name}' does not exist in settings")

        exchange_settings = self.exchanges_config[exchange_name]
        environment_paths = self.env_config.get('paths', {})

        self._exchange_config = ExchangeConfig(
            exchange_name=exchange_name,
            exchange_settings=exchange_settings,
            exchange_paths=self.exchange_paths,
            environment_paths=environment_paths
        )

    def get_logger(self, bot: bool = False):
        """
        Retrieves or creates a logger based on the current app and exchange.

        Parameters:
            bot (bool): Indicates whether to include Telegram settings.

        Returns:
            loguru.Logger: The configured logger instance.
        """
        if self._exchange_config:
            exchange_name = self._exchange_config.cfg.get('exchange_name', 'no_exchange')
        else:
            exchange_name = 'default'  # Use 'default' when no exchange is set

        logger_key = f"{self.cfg.get('app_name', 'default')}_{exchange_name}"

        if logger_key in self._loggers:
            return self._loggers[logger_key]

        log_filename = f"{self.cfg.get('app_name', 'default')}_{exchange_name}.log"
        log_path = str(Path(self.cfg.get('paths', {}).get('zeus_log', '')) / log_filename)

        telegram_settings = self.cfg.get('telegram_settings', {}) if bot else None

        setup_logger(
            log_path=log_path,
            app_name=self.cfg.get('app_name', 'default'),
            exchange_name=exchange_name,
            telegram_settings=telegram_settings
        )

        self._loggers[logger_key] = logger
        return logger

    def get_warehouse(self):
        from utils.warehouse import WareHouse
        if self._warehouse is None:
            self._warehouse = WareHouse(self)
        return self._warehouse

    def get_ch_pool(self):
        """
        Get or create a ClickHousePool instance.
        """
        if self._clickhouse_pool is None:
            from .cl_pool import ClickHousePool
            self._clickhouse_pool = ClickHousePool(self)
        return self._clickhouse_pool