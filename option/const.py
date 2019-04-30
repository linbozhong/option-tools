# coding:utf-8


OPT_OPTION_BASIC = 'opt_option_basic'
OPT_OPTION_DAILY = 'opt_option_daily'
OPT_OPTION_BAR = 'opt_option_bar'
OPT_TRADE_DAY = 'opt_trade_day'
OPT_UNDERLYING = 'opt_underlying'
OPT_VIX = 'opt_vix'

OPT_VIX_DAILY = 'vix.daily'


LOG_BASIC_NEWEST = '[Basic] [Gateway:{}] [Underlying:{}] [Data is newest]'
LOG_BASIC_UPDATE = '[Basic] [Gateway:{}] [Underlying:{}] [latest:{}] [Records:{}]'

LOG_DAILY_NEWEST = '[Daily] [Gateway:{}] [Exchange:{}] [Date:{}] [Data is newest or None-trading-day]'
LOG_DAILY_UPDATE = '[Daily] [Gateway:{}] [Exchange:{}] [Date:{}] [Records:{}]'

LOG_BAR_NEWEST = '[Bar] [Gateway:{}] [Code:{}] [StartDateTime:{}] [Data is newest]'
LOG_BAR_UPDATE = '[Bar] [Gateway:{}] [Code:{}] [StartDateTime:{}] [Records:{}]'

LOG_UNDERLYING_UPDATE = '[Underlying] [Gateway:{}] [Frequency:{}] [Code:{}] [StartDateTime:{}] [Records:{}]'
LOG_UNDERLYING_NEWEST = '[Underlying] [Gateway:{}] [Frequency:{}] [Code:{}] [StartDateTime:{}] [Data is newest]'

LOG_TRADE_DAY_NEWEST = '[TradeDay] [Gateway:{}] [StartDate:{}] [Data is newest]'
LOG_TRADE_DAY_UPDATE = '[TradeDay] [Gateway:{}] [StartDate:{}] [Records:{}]'

LOG_VIX_UPDATE = '[Vix] [StartDate:{}] [Records:{}]'
LOG_VIX_NEWEST = '[Vix] [StartDate:{}] [Data is newest]'
LOG_VIX_ERROR = 'Can not get vix.'

