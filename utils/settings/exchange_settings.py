from typing import Dict, Any

class ExchangeConfig:
    def __init__(
            self,
            exchange_name: str,
            exchange_settings: Dict[str, Any],
            exchange_paths: Dict[str, str],
            environment_paths: Dict[str, str]
    ):
        self.cfg: Dict[str, Any] = {
            'exchange_name': exchange_name,
            **exchange_settings
        }

        # Process and add exchange paths
        for path_key, path_template in exchange_paths.items():
            formatted_path = path_template.format(
                zeus_data=environment_paths.get('zeus_data', ''),
                exchange_name=exchange_name
            )
            self.cfg[path_key] = formatted_path


class ExchangeProxy:
    """
    A proxy that provides access to the exchange configuration
    or raises an error if the exchange is not set.
    """

    def __init__(self, app_settings):
        self.app_settings = app_settings

    @property
    def cfg(self) -> Dict[str, Any]:
        if not self.app_settings._exchange_config:
            raise ValueError(
                "Exchange is not set. Please use `set_exchange` to specify an exchange."
            )
        return self.app_settings._exchange_config.cfg

    def __getattr__(self, name: str) -> Any:
        if not self.app_settings._exchange_config:
            raise ValueError(
                f"Exchange is not set. Please use `set_exchange` to specify an exchange "
                f"before accessing '{name}'."
            )
        return getattr(self.app_settings._exchange_config.cfg, name)