# coding:utf-8
import os
from dotenv import load_dotenv

load_dotenv()


class BaseConnectConfig(object):
    URL = ''
    ID = ''
    TOKEN = ''


class JqdataConnectConfig(BaseConnectConfig):
    ID = os.getenv('JQDATA_ID')
    TOKEN = os.getenv('JQDATA_TOKEN')


class TushareConnectConfig(BaseConnectConfig):
    ID = os.getenv('TUSHARE_ID')
    TOKEN = os.getenv('TUSHARE_TOKEN')


connect_config = {
    'jqdata': JqdataConnectConfig,
    'tushare': TushareConnectConfig
}

if __name__ == '__main__':
    print(os.getenv('JQDATA_ID'))
    print(os.getenv('JQDATA_TOKEN'))
