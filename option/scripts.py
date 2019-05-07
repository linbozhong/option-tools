# coding:utf-8

import pymongo
import jqdatasdk as jq
import tushare as ts
import pandas as pd
import calendar
import requests
import csv
from io import StringIO
from dateutil.relativedelta import relativedelta
from datetime import datetime, date, time, timedelta
from jqdatasdk import opt, query
from option.settings import connect_config, OPTION_BASIC_FIELD, OPTION_DAILY_FIELD, BAR_PREV_TRADE_DAYS, QVIX_URL
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
    col = client[db_name][col_name]
    if index_filed is not None:
        if not col.index_information():
            index_list = [(field, pymongo.ASCENDING) for field in index_filed]
            col.create_index(index_list)
    data_list = data_df.to_dict(orient='records')
    col.insert_many(data_list)


def get_db_latest_record(db_name, col_name, field=None, *args, **kwargs):
    """
    :param db_name: str
    :param col_name: str
    :param field: str
    :return: dict
    """
    client = get_mongo_client()
    col = client[db_name][col_name]
    if field is None:
        field = '_id'
    if not col.count():
        doc = None
    else:
        cursor = col.find(*args, **kwargs).limit(1).sort(field, pymongo.DESCENDING)
        doc = None if cursor.count() == 0 else cursor.next()
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
    """
    :param gateway_name: str
    :param data_df: pandas.DataFrame
    :return: pandas.DataFrame
    """
    if gateway_name == 'jqdata':
        data_df['datetime'] = data_df.index
        return data_df
    elif gateway_name == 'tushare':
        pass


def get_trade_days(gateway_name, start_date):
    """
    :param gateway_name: str
    :param start_date: datetime/date/str
    :return: pandas.DataFrame
    """
    sdk = get_data_sdk(gateway_name)
    if gateway_name == 'jqdata':
        trade_days = sdk.get_trade_days(start_date=start_date)
        trade_days = [date_to_dt(date) for date in trade_days]
        data_df = pd.DataFrame(trade_days, columns=['date'])
        return data_df


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
    """
    :param gateway_name: str
    :param code: str
    :param start_date: datetime/date/str
    :param end_date: datetime/date/str
    :return: pandas.DataFrame
    """
    sdk = get_data_sdk(gateway_name)
    if gateway_name == 'jqdata':
        df = sdk.get_price(code, start_date=start_date, end_date=end_date, frequency='1m')
        df.dropna(inplace=True)
        df['code'] = code
        return df
    elif gateway_name == 'tushare':
        pass


def get_underlying(gateway_name, code, start_date, end_date, freq):
    """
    :param gateway_name: str
    :param code: str
    :param start_date: date/datetime/str
    :param end_date: date/datetime/str
    :param freq: str
    :return: pandas.DataFrame
    """
    sdk = get_data_sdk(gateway_name)
    if gateway_name == 'jqdata':
        code = '{}.{}'.format(code, UNDERLYING_EXCHANGE_MAP[code])
        df = sdk.get_price(code.upper(), start_date, end_date, frequency=freq)
        df['datetime'] = df.index
        return df
    elif gateway_name == 'tushare':
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
    output_field = ['code', 'trading_code', 'underlying_symbol', 'list_date', 'last_trade_date']
    cursor = get_db_records(OPT_OPTION_BASIC, underlying_symbol, filter=flt, projection=output_field)

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


def save_trade_days(gateway_name):
    """
    :param gateway_name: str
    :return:
    """
    latest_record = get_db_latest_record(OPT_TRADE_DAY, OPT_TRADE_DAY)
    latest_date = datetime(2005, 1, 1) if latest_record is None else latest_record['date']
    start_date = latest_date + timedelta(days=1)
    data_df = get_trade_days(gateway_name, start_date)
    if not data_df.empty:
        insert_to_db(data_df, OPT_TRADE_DAY, OPT_TRADE_DAY, index_filed=['date'])
        logger.info(LOG_TRADE_DAY_UPDATE.format(gateway_name,
                                                dt_to_str(start_date),
                                                len(data_df)))
    else:
        logger.info(LOG_TRADE_DAY_NEWEST.format(gateway_name,
                                                dt_to_str(start_date)))


def save_underlying(gateway_name, code, freq):
    """
    :param gateway_name: str
    :param code: str
    :param freq: str. 'daily' or '1m'
    :return:
    """
    if freq == 'daily':
        col_name = '{}.daily'.format(code)
        delta = timedelta(days=1)
    else:
        col_name = '{}.bar'.format(code)
        delta = timedelta(minutes=1)

    latest_record = get_db_latest_record(OPT_UNDERLYING, col_name)
    latest_date = datetime(2019, 4, 19) if latest_record is None else latest_record['datetime']
    start_date = latest_date + delta
    end_date = datetime.combine(date.today(), time.min).replace(hour=16)
    data_df = get_underlying(gateway_name, code, start_date, end_date, freq)
    if not data_df.empty:
        insert_to_db(data_df, OPT_UNDERLYING, col_name, index_filed=['datetime'])
        logger.info(LOG_UNDERLYING_UPDATE.format(gateway_name,
                                                 freq,
                                                 code,
                                                 dt_to_str(start_date),
                                                 len(data_df)))
    else:
        logger.info(LOG_UNDERLYING_NEWEST.format(gateway_name,
                                                 freq,
                                                 code,
                                                 dt_to_str(start_date)))


def save_option_basic(gateway_name, underlying_symbol):
    """
    :param gateway_name: str
    :param underlying_symbol: str
    :return:
    """
    latest_record = get_db_latest_record(OPT_OPTION_BASIC, underlying_symbol)
    latest_id = 0 if latest_record is None else latest_record['id']
    data_df = get_option_basic(gateway_name, underlying_symbol, latest_id)
    if not data_df.empty:
        data_df = normalize_basic_format(gateway_name, data_df)
        insert_to_db(data_df, OPT_OPTION_BASIC, underlying_symbol,
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
    latest_record = get_db_latest_record(OPT_OPTION_DAILY, exchange_code)
    latest_date = str_to_dt('2019-04-19') if latest_record is None else latest_record['date']
    today = datetime.combine(date.today(), time.min)
    if latest_date < today:
        while latest_date < today:
            latest_date += timedelta(days=1)
            data_df = get_option_daily(gateway_name, exchange_code, dt_to_str(latest_date))
            if not data_df.empty:
                data_df = normalize_daily_format(gateway_name, data_df)
                insert_to_db(data_df, OPT_OPTION_DAILY, exchange_code, index_filed=['date', 'code'])
                logger.info(LOG_DAILY_UPDATE.format(gateway_name,
                                                    exchange_code,
                                                    dt_to_str(latest_date),
                                                    len(data_df)))
            else:
                logger.info(LOG_DAILY_NEWEST.format(gateway_name,
                                                    exchange_code,
                                                    dt_to_str(latest_date)))


def save_option_bar(gateway_name, underlying_symbol):
    """
    :param gateway_name: str
    :param underlying_symbol: str
    :return:
    """
    today = datetime.combine(date.today(), time.min)
    prev_trade_days = get_db_records(OPT_TRADE_DAY, OPT_TRADE_DAY, {'date': {'$lte': today}})
    bar_days = [rec['date'] for rec in list(prev_trade_days)[-BAR_PREV_TRADE_DAYS:]]
    init_start_date = bar_days[0]
    end_date = bar_days[-1]

    near_list, far_list = read_near_and_far_contracts(underlying_symbol)
    all_list = []
    all_list.extend(near_list)
    all_list.extend(far_list)
    for contract in all_list:
        code = contract['code']
        flt = {'code': code}
        latest_record = get_db_latest_record(OPT_OPTION_BAR, underlying_symbol, filter=flt)
        start_date = init_start_date if latest_record is None else latest_record['datetime'] + timedelta(minutes=1)
        if start_date > end_date.replace(hour=15):
            logger.info('{}: Bar Trading Timestamp is newest.'.format(code))
        else:
            data_df = get_option_bar(gateway_name, code, start_date, end_date.replace(hour=16))
            if not data_df.empty:
                data_df = normalize_bar_format(gateway_name, data_df)
                insert_to_db(data_df, OPT_OPTION_BAR, underlying_symbol, index_filed=['code', 'datetime'])
                logger.info(LOG_BAR_UPDATE.format(gateway_name,
                                                  code,
                                                  start_date,
                                                  len(data_df)))
            else:
                logger.info(LOG_BAR_NEWEST.format(gateway_name,
                                                  code,
                                                  start_date))


def get_vix():
    resp = requests.get(QVIX_URL)
    data_file = StringIO(resp.text)
    csv_reader = csv.DictReader(data_file)
    csv_list = list(csv_reader)
    df = pd.DataFrame(csv_list)
    df.columns = ['datetime', 'open', 'high', 'low', 'close']
    df.datetime = df.datetime.map(lambda tstamp: datetime.fromtimestamp(float(tstamp) / 1000))
    return df


def save_vix():
    latest_record = get_db_latest_record(OPT_VIX, OPT_VIX_DAILY)
    latest_date = datetime(2015, 2, 1) if latest_record is None else latest_record['datetime']
    data_df = get_vix()
    if not data_df.empty:
        data_df = data_df[data_df.datetime > latest_date]
        if not data_df.empty:
            insert_to_db(data_df, OPT_VIX, OPT_VIX_DAILY, index_filed=['datetime'])
            logger.info(LOG_VIX_UPDATE.format(dt_to_str(latest_date),
                                              len(data_df)))
        else:
            logger.info(LOG_VIX_NEWEST.format(dt_to_str(latest_date)))

    else:
        logger.info(LOG_VIX_ERROR)


if __name__ == '__main__':
    save_trade_days('jqdata')

    save_option_basic('jqdata', '510050')
    save_option_daily('jqdata', 'XSHG')

    save_underlying('jqdata', '510050', 'daily')
    save_underlying('jqdata', '510050', '1m')
    # df.to_csv('basic_test.csv')

    save_vix()

    # l = read_month_contracts('510050', 2019, 6)
    # [print(i) for i in l]
    # print(len(l))

    # near, far = read_near_and_far_contracts('510050')
    # [print(i) for i in near]
    # [print(i) for i in far]

    # save_option_bar('jqdata', '510050')
