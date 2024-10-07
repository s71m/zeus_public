"""
Settings Package

This package provides configuration management for applications
and exchanges, including logging setup.
"""

__version__ = '1.0.0'

from .settings import Settings, get_app
from .app import App
from .exchange_settings import ExchangeConfig, ExchangeProxy
from .logger_config import setup_logger, AsyncTelegramHandler

__all__ = [
    'Settings',
    'get_app',
    'App',
    'ExchangeConfig',
    'ExchangeProxy',
    'setup_logger',
    'AsyncTelegramHandler'
]