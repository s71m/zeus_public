import logging
import pandas as pd
from db.redis.redis_timeseries_manager import RedisTimeseriesManager

from settings_archive import settings

logger = logging.getLogger(__name__)
frm = "%(asctime)s %(levelname)s --- (%(filename)s).%(funcName)s(%(lineno)d):\t %(message)s"
logging.basicConfig(format=frm, level=logging.DEBUG)


class MarketData(RedisTimeseriesManager):
    _name = 'market'
    _lines = ['close', 'price', 'quantity']
    # _lines = ['open', 'high', 'low', 'close', 'volume']
    _timeframes = {
        'raw': {'retention_secs': 100000},  # unlimited
    }


md = MarketData(**settings.redis_md_settings)


def set_r_order(account_id, instrument_uid, data):
    success, value = md.insert(
        data=data,
        c1=account_id,
        c2=instrument_uid,
        create_inplace=True,
    )
    if not success:
        logger.error(success)
        raise Exception(data)


def get_r_order_uid_df(account_id, instrument_uid):
    success, df = md.read(
        c1=account_id,
        c2=instrument_uid,
        timeframe='raw',
        allow_multiple=True,
        return_as='df'
    )
    if not success:
        return pd.DataFrame()
    df = df_rename_cols(df)
    return df


def get_r_order_account_df(account_id):
    success, df = md.read(
        c1=account_id,
        timeframe='raw',
        allow_multiple=True,
        return_as='df'
    )
    if not success:
        return pd.DataFrame()
    df = df_rename_cols(df)
    return df


def get_r_order_df():
    success, df = md.read(
        timeframe='raw',
        allow_multiple=True,
        return_as='df'
    )
    if not success:
        return pd.DataFrame()
    df = df_rename_cols(df)
    return df


def delete_order_uid(account_id, instrument_uid):
    md.delete_classifier_keys(c1=account_id, c2=instrument_uid)


def get_avg_close_uid(account_id, instrument_uid):
    df = get_r_order_uid_df(account_id=account_id, instrument_uid=instrument_uid)
    if not df.empty:
        # avg_candle_close = (df['close'] * df['quantity']).sum() / df['quantity'].sum()
        avg_candle_close = (df['close'] * df['quantity']).sum() / df['quantity'].sum()
        avg_executed_price = (df['price'] * df['quantity']).sum() / df['quantity'].sum()
        size_prev_orders = df.index.size
        return size_prev_orders, avg_candle_close, avg_executed_price


def check_connect():
    if not md.client.ping():
        logger.error("Redis is not connected")
        raise Exception("Redis is not connected")

    print("Redis is connected")


def df_rename_cols(df):
    df.rename(columns={'c1': 'account_id', 'c2': 'instrument_uid'}, inplace=True)
    return df


if __name__ == '__main__':
    df = get_r_order_df()
    print(df)
