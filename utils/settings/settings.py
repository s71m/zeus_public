from pathlib import Path
from typing import Dict, Optional, Any, ClassVar
import yaml
from loguru import logger

from .app import App


class Settings:
    _instance: ClassVar[Optional['Settings']] = None

    def __init__(self):
        self.config = self._load_config()
        self.env = self.config.get("APP_ENV", "dev")
        self._apps: Dict[str, App] = {}

    @staticmethod
    def _load_config() -> Dict[str, Any]:
        config_path = Path(__file__).parent / "settings.yaml"
        secrets_path = Path(__file__).parent / "secrets.yaml"

        try:
            config = yaml.safe_load(config_path.read_text())

            if secrets_path.exists():
                secrets = yaml.safe_load(secrets_path.read_text())
                config = Settings._deep_merge(config, secrets)
            else:
                logger.warning(f"Secrets file not found at {secrets_path}")

            return config
        except yaml.YAMLError as e:
            logger.error(f"Error loading configuration: {e}")
            raise

    @staticmethod
    def _deep_merge(dict1: Dict, dict2: Dict) -> Dict:
        """Recursively merge two dictionaries."""
        result = dict1.copy()
        for key, value in dict2.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = Settings._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    @classmethod
    def get_instance(cls) -> 'Settings':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        cls._instance = None

    def get_app(self, app_name: str, exchange_name: Optional[str] = None) -> App:
        if app_name not in self._apps:
            if app_name not in self.config.get('apps', {}):
                raise ValueError(f"Settings not found for application: {app_name}")

            app_config = self.config['apps'][app_name]
            env_config = self.config.get('environment', {}).get(self.env, {})
            exchanges_config = self.config.get('exchanges', {})
            exchange_paths = self.config.get('exchange_paths', {})

            app_settings = App(
                app_name=app_name,
                global_config=self.config,
                env_config=env_config,
                app_config=app_config,
                exchanges_config=exchanges_config,
                exchange_paths=exchange_paths
            )
            self._apps[app_name] = app_settings

        app = self._apps[app_name]
        if exchange_name:
            try:
                app.set_exchange(exchange_name)
            except ValueError as e:
                logger.error(f"Failed to set exchange '{exchange_name}' for app '{app_name}': {e}")
                raise
        return app


def get_app(app_name: str = 'default', exchange_name: Optional[str] = None) -> App:
    """
    Retrieves an AppSettings instance for the specified app and exchange.

    Parameters:
        app_name (str): The name of the application to retrieve settings for.
        exchange_name (Optional[str]): The name of the exchange to set for the application.

    Returns:
        AppSettings: The settings instance for the specified application.
    """
    settings = Settings.get_instance()
    return settings.get_app(app_name, exchange_name)