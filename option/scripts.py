# coding:utf-8

import pymongo
import jqdatasdk as jq
import tushare as ts
import pandas as pd

from datetime import datetime, date, time, timedelta
from jqdatasdk import opt, query
from option.setting import connect_config
from option.common import str_to_dt, dt_to_str, date_to_dt
from option.log import logger

OPTION_BASIC = 'option_basic'
OPTION_DAILY = 'option_daily'
OPTION_BAR = 'option_bar'

OPTION_BASIC_FIELD = [
    'id',
    'code',
    'trading_code',
    'name',
    'contract_type',
    'exchange_code',
    'underlying_symbol',
    'underlying_type',
    'exercise_price',
    'contract_unit',
    'list_date',
    'last_trade_date'
]

OPTION_DAILY_FIELD = [
    'code',
    'exchange_code',
    'date',
    'open',
    'high',
    'low',
    'close',
    'settle_price',
    'volume',
    'money',
    'position'
]

UNDERLYING_EXCHANGE_MAP = {
    '510050': 'XSHG'
}

TUSHARE_EXCHANGE_CODE_MAP = {
    'SSE': 'XSHG'
}

sdk_map = {
    'jqdata': None,
    'tushare': None
}


def connect_api(gateway_name):
    config = connect_config[gateway_name]
    if gateway_name == 'jqdata':
        jq.auth(config.ID, config.TOKEN)
        return jq
    elif gateway_name == 'tushare':
        pro = ts.pro_api(config.TOKEN)
        return pro


def get_data_sdk(gateway_name):
    sdk = sdk_map.get(gateway_name)
    if sdk is None:
        sdk = connect_api(gateway_name)
        sdk_map[gateway_name] = sdk
    return sdk


def get_mongo_client(host='localhost', port=27017):
    client = pymongo.MongoClient(host=host, port=port)
    return client


def insert_to_db(data_df, client, db_name, col_name, index_filed=None):
    db = client[db_name]
    col = db[col_name]
    if index_filed is not None:
        index_list = [(field, pymongo.ASCENDING) for field in index_filed]
        col.create_index(index_list)
    data_list = data_df.to_dict(orient='records')
    col.insert_many(data_list)


def get_db_latest_record(client, db_name, col_name):
    col = client[db_name][col_name]
    if not col.count():
        doc = None
    else:
        cursor = col.find().limit(1).sort('_id', pymongo.DESCENDING)
        doc = cursor.next()
    return doc


def normalize_basic_format(gateway_name, data_df):
    if gateway_name == 'jqdata':
        for field in ['code', 'underlying_symbol']:
            data_df[field] = data_df[field].map(lambda code: code.split('.')[0])
        for field in ['list_date', 'last_trade_date']:
            data_df[field] = data_df[field].map(date_to_dt)
        data_df = data_df[OPTION_BASIC_FIELD]
        return data_df
    if gateway_name == 'tushare':
        pass


def normalize_daily_format(gateway_name, data_df):
    if gateway_name == 'jqdata':
        data_df['date'] = data_df['date'].map(date_to_dt)
        data_df = data_df[OPTION_DAILY_FIELD]
        return data_df
    elif gateway_name == 'tushare':
        pass


def get_option_basic(gateway_name, underlying_symbol, latest_id):
    sdk = get_data_sdk(gateway_name)
    if gateway_name == 'jqdata':
        underlying_symbol = '{}.{}'.format(underlying_symbol, UNDERLYING_EXCHANGE_MAP[underlying_symbol])
        df_sum = pd.DataFrame()
        while True:
            table = opt.OPT_CONTRACT_INFO
            q = query(table).filter(table.underlying_symbol == underlying_symbol.upper(), table.id > latest_id)
            df = sdk.opt.run_query(q)
            if df.empty:
                break
            df_sum = df_sum.append(df)
            latest_id = int(df.iloc[-1].id)
        return df_sum
    elif gateway_name == 'tushare':
        pass


def get_option_daily(gateway_name, exchange_code, trade_date):
    sdk = get_data_sdk(gateway_name)
    if gateway_name == 'jqdata':
        table = opt.OPT_DAILY_PRICE
        q = query(table).filter(table.exchange_code == exchange_code.upper(), table.date == trade_date)
        df = sdk.opt.run_query(q)
        return df
    elif gateway_name == 'tushare':
        pass


def save_option_basic(gateway_name, underlying_symbol):
    client = get_mongo_client()
    latest_record = get_db_latest_record(client, OPTION_BASIC, underlying_symbol)
    latest_id = 0 if latest_record is None else latest_record['id']
    data_df = get_option_basic(gateway_name, underlying_symbol, latest_id)
    if not data_df.empty:
        data_df = normalize_basic_format(gateway_name, data_df)
        insert_to_db(data_df, client, OPTION_BASIC, underlying_symbol,
                     index_filed=['id', 'list_date', 'last_trade_date'])
        logger.info(
            '[Basic] [Gateway:{}] [Underlying:{}] [latest:{}] [Records:{}]'.format(gateway_name,
                                                                                   underlying_symbol,
                                                                                   latest_id,
                                                                                   len(data_df)))
    else:
        logger.info('[Basic] [Gateway:{}] [Underlying:{}] data is newest.'.format(gateway_name, underlying_symbol))


def save_option_daily(gateway_name, exchange_code):
    client = get_mongo_client()
    latest_record = get_db_latest_record(client, OPTION_DAILY, exchange_code)
    latest_date = str_to_dt('2019-04-19') if latest_record is None else latest_record['date']
    today = datetime.combine(date.today(), time.min)
    while latest_date < today:
        data_df = get_option_daily(gateway_name, exchange_code, dt_to_str(latest_date))
        if not data_df.empty:
            data_df = normalize_daily_format(gateway_name, data_df)
            insert_to_db(data_df, client, OPTION_DAILY, exchange_code, index_filed=['date', 'code'])
            logger.info('[Daily] [Gateway:{}] [Exchange:{}] [Date:{}] [Records:{}]'.format(gateway_name,
                                                                                           exchange_code,
                                                                                           dt_to_str(latest_date),
                                                                                           len(data_df)))
        latest_date += timedelta(days=1)


if __name__ == '__main__':
    save_option_basic('jqdata', '510050')
    save_option_daily('jqdata', 'XSHG')
    # df.to_csv('basic_test.csv')
