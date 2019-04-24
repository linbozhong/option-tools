# coding:utf-8

import pymongo
import jqdatasdk as jq
import tushare as ts
import pandas as pd

from datetime import datetime
from jqdatasdk import opt, query
from option.setting import connect_config
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

UNDERLYING_EXCHANGE_MAP = {
    '510050': 'XSHG'
}

TUSHARE_EXCHANGE_CODE_MAP = {
    'SSE': 'XSHG'
}


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
            data_df[field] = data_df[field].map(lambda date: datetime(date.year, date.month, date.day))
        data_df = data_df[OPTION_BASIC_FIELD]
        return data_df
    if gateway_name == 'tushare':
        pass


def connect_api(gateway_name):
    config = connect_config[gateway_name]
    if gateway_name == 'jqdata':
        jq.auth(config.ID, config.TOKEN)
        return jq
    elif gateway_name == 'tushare':
        pro = ts.pro_api(config.TOKEN)
        return pro


def get_option_basic(gateway_name, underlying_symbol, latest_id):
    sdk = connect_api(gateway_name)
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


def save_option_basic(gateway_name, underlying_symbol):
    client = get_mongo_client()
    latest_record = get_db_latest_record(client, OPTION_BASIC, underlying_symbol)
    if latest_record is None:
        latest_id = 0
    else:
        latest_id = latest_record['id']
    data_df = get_option_basic(gateway_name, underlying_symbol, latest_id)
    if not data_df.empty:
        data_df = normalize_basic_format(gateway_name, data_df)
        insert_to_db(data_df, client, OPTION_BASIC, underlying_symbol,
                     index_filed=['id', 'list_date', 'last_trade_date'])
        logger.info(
            'From {} get {}: Latest Id of DB is {} Fetch {} Records.'.format(gateway_name, underlying_symbol, latest_id,
                                                                             len(data_df)))
    else:
        logger.info('From {} get {}: Option Basic data is newest.'.format(gateway_name, underlying_symbol))


if __name__ == '__main__':
    save_option_basic('jqdata', '510050')
    # df.to_csv('basic_test.csv')
