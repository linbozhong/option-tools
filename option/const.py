# coding:utf-8


OPTION_BASIC = 'option_basic'
OPTION_DAILY = 'option_daily'
OPTION_BAR = 'option_bar'
TRADE_DAY = 'trade_day'

LOG_BASIC_NEWEST = '[Basic] [Gateway:{}] [Underlying:{}] [Data is newest]'
LOG_DAILY_NEWEST = '[Daily] [Gateway:{}] [Exchange:{}] [Date:{}] [Data is newest or None-trading-day]'
LOG_BAR_NEWEST = '[Bar] [Gateway:{}] [Code:{}] [StartDateTime:{}] [Data is newest]'
LOG_BASIC_UPDATE = '[Basic] [Gateway:{}] [Underlying:{}] [latest:{}] [Records:{}]'
LOG_DAILY_UPDATE = '[Daily] [Gateway:{}] [Exchange:{}] [Date:{}] [Records:{}]'
LOG_BAR_UPDATE = '[Bar] [Gateway:{}] [Code:{}] [StartDateTime:{}] [Records:{}]'

LOG_TRADE_DAY_NEWEST = '[TradeDay] [Gateway:{}] [StartDate:{}] [Data is newest]'
LOG_TRADE_DAY_UPDATE = '[TradeDay] [Gateway:{}] [StartDate:{}] [Records:{}]'
