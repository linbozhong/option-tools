# coding:utf-8

import pymongo
import jqdatasdk as jq
import tushare as ts
import pandas as pd
import calendar
from dateutil.relativedelta import relativedelta
from datetime import datetime, date, time, timedelta
from jqdatasdk import opt, query
from option.setting import connect_config, OPTION_BASIC_FIELD, OPTION_DAILY_FIELD
from option.common import str_to_dt, dt_to_str, date_to_dt
from option.log import logger
from option.const import *

UNDERLYING_EXCHANGE_MAP = {
    '510050': 'XSHG'
}

TUSHARE_EXCHANGE_CODE_MAP = {
    'SSE': 'XSHG'
}

global_object_map = dict()


def connect_api(gateway_name):
    """
    :param gateway_name: str
    :return: instance
    """
    config = connect_config[gateway_name]
    if gateway_name == 'jqdata':
        jq.auth(config.ID, config.TOKEN)
        return jq
    elif gateway_name == 'tushare':
        pro = ts.pro_api(config.TOKEN)
        return pro


def get_data_sdk(gateway_name):
    """
    :param gateway_name: str
    :return: instance
    """
    sdk_name = '{}_sdk'.format(gateway_name)
    sdk = global_object_map.get(sdk_name)
    if sdk is None:
        sdk = connect_api(gateway_name)
        global_object_map[sdk_name] = sdk
    return sdk


def get_mongo_client(host='localhost', port=27017):
    """
    :param host: str
    :param port: int
    :return: instance
    """
    client = global_object_map.get('mongo_client')
    if client is None:
        client = pymongo.MongoClient(host=host, port=port)
        global_object_map['mongo_client'] = client
    return client


def insert_to_db(data_df, db_name, col_name, index_filed=None):
    """
    :param data_df: pandas.DataFrame
    :param db_name: str
    :param col_name: str
    :param index_filed: list
    :return:
    """
    client = get_mongo_client()
    db = client[db_name]
    col = db[col_name]
    if index_filed is not None:
        index_list = [(field, pymongo.ASCENDING) for field in index_filed]
        col.create_index(index_list)
    data_list = data_df.to_dict(orient='records')
    col.insert_many(data_list)


def get_db_latest_record(db_name, col_name):
    """
    :param db_name: str
    :param col_name: str
    :return: dict
    """
    client = get_mongo_client()
    col = client[db_name][col_name]
    if not col.count():
        doc = None
    else:
        cursor = col.find().limit(1).sort('_id', pymongo.DESCENDING)
        doc = cursor.next()
    return doc


def get_db_records(db_name, col_name, *args, **kwargs):
    """
    :param db_name: str
    :param col_name: str
    :return: pymongo.Cursor
    """
    client = get_mongo_client()
    col = client[db_name][col_name]
    cursor = col.find(*args, **kwargs)
    return cursor


def normalize_basic_format(gateway_name, data_df):
    """
    :param gateway_name: str
    :param data_df: pandas.DataFrame
    :return: pandas.DataFrame
    """
    if gateway_name == 'jqdata':
        for field in ['list_date', 'last_trade_date']:
            data_df[field] = data_df[field].map(date_to_dt)
        data_df = data_df[OPTION_BASIC_FIELD]
        return data_df
    if gateway_name == 'tushare':
        pass


def normalize_daily_format(gateway_name, data_df):
    """
    :param gateway_name: str
    :param data_df: pandas.DataFrame
    :return: pandas.DataFrame
    """
    if gateway_name == 'jqdata':
        data_df['date'] = data_df['date'].map(date_to_dt)
        data_df = data_df[OPTION_DAILY_FIELD]
        return data_df
    elif gateway_name == 'tushare':
        pass


def normalize_bar_format(gateway_name, data_df):
    pass


def get_option_basic(gateway_name, underlying_symbol, latest_id):
    """
    :param gateway_name: str
    :param underlying_symbol: str
    :param latest_id: int
    :return: pandas.DataFrame
    """
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
    """
    :param gateway_name: str
    :param exchange_code: str
    :param trade_date: str. '%Y-%m-%d'
    :return: pandas.DataFrame
    """
    sdk = get_data_sdk(gateway_name)
    if gateway_name == 'jqdata':
        table = opt.OPT_DAILY_PRICE
        q = query(table).filter(table.exchange_code == exchange_code.upper(), table.date == trade_date)
        df = sdk.opt.run_query(q)
        return df
    elif gateway_name == 'tushare':
        pass


def get_option_bar(gateway_name, code, start_date, end_date):
    pass


def read_month_contracts(underlying_symbol, year, month):
    """
    :param underlying_symbol: str
    :param year: int
    :param month: int
    :return: list
    """
    _, last = calendar.monthrange(year, month)
    first_date = datetime(year, month, 1)
    last_date = datetime(year, month, last)

    flt = {'last_trade_date': {'$gt': first_date, '$lt': last_date}}
    output_field = ['code', 'trading_code', 'list_date', 'last_trade_date']
    cursor = get_db_records(OPTION_BASIC, underlying_symbol, filter=flt, projection=output_field)

    res_list = [rec for rec in cursor if 'A' not in rec['trading_code']]
    return res_list


def read_near_and_far_contracts(underlying_symbol):
    """
    :param underlying_symbol: str
    :return: tuple(list, list)
    """
    today = datetime.combine(date.today(), time.min)
    today_next_month = today + relativedelta(months=1)
    today_next2_month = today_next_month + relativedelta(months=1)

    near_list = read_month_contracts(underlying_symbol, today.year, today.month)
    last_date = near_list[0]['last_trade_date']
    if today > last_date:
        near_list = read_month_contracts(underlying_symbol, today_next_month.year, today_next_month.month)
        far_list = read_month_contracts(underlying_symbol, today_next2_month.year, today_next2_month.month)
    else:
        far_list = read_month_contracts(underlying_symbol, today_next_month.year, today_next_month.month)
    return near_list, far_list


def save_option_basic(gateway_name, underlying_symbol):
    """
    :param gateway_name: str
    :param underlying_symbol: str
    :return:
    """
    latest_record = get_db_latest_record(OPTION_BASIC, underlying_symbol)
    latest_id = 0 if latest_record is None else latest_record['id']
    data_df = get_option_basic(gateway_name, underlying_symbol, latest_id)
    if not data_df.empty:
        data_df = normalize_basic_format(gateway_name, data_df)
        insert_to_db(data_df, OPTION_BASIC, underlying_symbol,
                     index_filed=['id', 'list_date', 'last_trade_date'])
        logger.info(LOG_BASIC_UPDATE.format(gateway_name,
                                            underlying_symbol,
                                            latest_id,
                                            len(data_df)))
    else:
        logger.info(LOG_BASIC_NEWEST.format(gateway_name, underlying_symbol))


def save_option_daily(gateway_name, exchange_code):
    """
    :param gateway_name: str
    :param exchange_code: str
    :return:
    """
    latest_record = get_db_latest_record(OPTION_DAILY, exchange_code)
    latest_date = str_to_dt('2019-04-19') if latest_record is None else latest_record['date']
    today = datetime.combine(date.today(), time.min)
    if latest_date >= today:
        logger.info(LOG_DAILY_NEWEST.format(gateway_name, exchange_code))
    else:
        while latest_date < today:
            latest_date += timedelta(days=1)
            data_df = get_option_daily(gateway_name, exchange_code, dt_to_str(latest_date))
            if not data_df.empty:
                data_df = normalize_daily_format(gateway_name, data_df)
                insert_to_db(data_df, OPTION_DAILY, exchange_code, index_filed=['date', 'code'])
                logger.info(LOG_DAILY_UPDATE.format(gateway_name,
                                                    exchange_code,
                                                    dt_to_str(latest_date),
                                                    len(data_df)))


if __name__ == '__main__':
    save_option_basic('jqdata', '510050')
    save_option_daily('jqdata', 'XSHG')
    # df.to_csv('basic_test.csv')

    # l = read_month_contracts('510050', 2019, 6)
    # [print(i) for i in l]
    # print(len(l))

    near, far = read_near_and_far_contracts('510050')
    [print(i) for i in near]
    [print(i) for i in far]
