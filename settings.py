import os

# GENERAL
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DEBUG = True

LOG_PATH_MAC = 'logs/'
LOG_PATH = 'D:/Logs/'

DATA_PATH = '/Users/connahcutbush/Documents/data/div_harvest'
OUTPUT_PATH = '/Users/connahcutbush/Documents/data/div_harvest/output/'
DEFAULT_TRADE_FILE = 'trades.csv'
DEFAULT_PNL_FILE = 'pnl.csv'
DEFAULT_PORT_FILE = 'port.csv'
USE_CACHE = True

COMPANY_TAX_RATE = 0.3

BACKTEST_START_DATE = '2016-01-01'

# filter levels
LIMIT_DY = 0.025
LIMIT_ER = 0.00
LIMIT_VOLATILITY = 0.27
DAYS_TO_HOLD = 51

# Fund size values
FUND_SIZE = 300000
BUCKET_WEIGHT_1 = 0.075
BUCKET_WEIGHT_2 = 0.025
BUCKET_WEIGHT_3 = 0.015

# Trade cost constants
GROSS_EXPOSURE = 0
NET_EXPOSURE = 0
BBSW = 0.017
TRADE_COMMISSION = 0.00005
LONG_FUNDING_COST_BBSW = 0.005
SHORT_COST = 0.0045

# csv files for data
UNIV_FILE = 'index_data.csv'
META_FILE = 'd_asx200-meta.csv'
DIVIDENDS_FILE = 'dividend_history.csv'
# DIVIDENDS_FILE = 'dps_estimates_clean.csv'
PRICES_FILE = 'd_asx200-adjprice.csv'

# FTP
FTP_DETAILS = {
    "ftp1": {
        "host": "ftp.host.com",
        "port": 21,
        "user": "u",
        "password": "",
        "local_storage_path": "temp/ftp_downloads"
    },
    "ftp2": {
        "host": "ftp.host.com",
        "port": 21,
        "user": "funddata",
        "password": "FRVvar",
        "local_storage_path": "temp/downloads",
        "log_table": "files_imported",
        "remote_path": "mydata/",
    },

}

# DATABASE
DATABASES = {
    "default": {
        "server": "mssql+pymssql",
        "host": "172.31.1.221",
        "port": "2433",
        "user": "dataworker",
        "password": "dataworker",
        "name": {
            "fds": "FDS_Standard",
            "supplemental": "FDS_Supplemental",
            "constituents": "constituents",
            "utility": "utility",
            "benchmark": "FDS_Benchmark",
        }
    },
    "QADirect": {
        "server": "mssql+pymssql",
        "host": "54.195.20.206",
        "port": "2866",
        "user": "dataworker",
        "password": "dataworker",
        "name": {
            "fds": "FDS_Standard",
            "supplemental": "FDS_Supplemental",
            "constituents": "constituents",
            "utility": "utility",
            "benchmark": "FDS_Benchmark",
        }
    },
    "mongodb": {
        "server": "prod01",
        "port": 27017,
        "name": {
            "portfolio": "portfolio"
        }
    }
}
