__author__ = 'connahcutbush'

import datetime
import numpy as np
import os
import pandas as pd
import pickle
from pandas.tseries.offsets import BDay

import settings
from data_access.utility.logging import get_logger


class data_factory(object):
    def __init__(self, logger=None):

        self._server = settings.DATABASES["default"]["host"]
        self._username = settings.DATABASES["default"]["user"]
        self._password = settings.DATABASES["default"]["password"]
        self._database = settings.DATABASES["default"]["name"]["fds"]
        self._data_path = settings.DATA_PATH
        self._meta_file = settings.META_FILE
        self._univ_file = settings.UNIV_FILE
        self._dividend_file = settings.DIVIDENDS_FILE
        self._price_file = settings.PRICES_FILE
        self.fast_prices = {}

        self._meta_cols = ['fs_perm_sec_id', 'ticker_exchange', 'sedol', 'sector_code', 'ff_co_name']

        if not logger:
            logger = get_logger()
        self._logger = logger

        self.data_files = {'return_p': 'd_asx200-return_p.csv', 'return_t': 'd_asx200-return_t.csv',
                           'adv20': 'd_asx200-adv20.csv', 'divs_raw': 'd_asx200-divs.csv',
                           'div_adj': 'd_asx200-divsadj.csv', 'mval': 'd_asx200-mval.csv',
                           'shares': 'd_asx200-shares.csv', 'sp_divs': 'd_asx200-sp_divs.csv',
                           'sp_divsadj': 'd_asx200-sp_divsadj.csv', 'split_factor': 'd_asx200-split_factor.csv',
                           'spo_adj_price': 'd_asx200-spo_adj_price.csv', 'volume': 'd_asx200-volume.csv',
                           'hist_dy': 'd_asx200-histdy.csv', 'eps_fy1': 'd_asx200-eps_fy1.csv',
                           'eps_fy2': 'd_asx200-eps_fy2.csv', 'epsntma': 'd_asx200-epsntma.csv',
                           'ey_fy1': 'd_asx200-ey_fy1.csv', 'ey_fy2': 'd_asx200-ey_fy2.csv',
                           'eyntma': 'd_asx200-eyntma.csv', 'div_adj_ann': 'd_asx200-divsadjann.csv'}

        self.universe = self.get_universe_from_csv()
        self.metadata = self.get_metadata_from_csv()
        self.dataset = pd.DataFrame()
        self.prices = pd.DataFrame()

    def build_prices(self, use_cache):
        if use_cache:
            self.fast_prices = pickle.load(open(os.path.join(self._data_path, 'fast_prices.p'), 'rb'))
        else:
            # sort the dataframe
            p = self.prices[['stock_id', 'price_date', 'close_price']]

            p = p.reset_index()
            p.sort_values(by=['fs_perm_sec_id'], inplace=True)
            # set the index to be this and don't drop
            p.set_index(keys=['fs_perm_sec_id'], drop=False, inplace=True)
            # get a list of names
            stock_ids = p['fs_perm_sec_id'].unique().tolist()
            # now we can perform a lookup on a 'view' of the dataframe
            for stock_id in stock_ids:
                pseries = p.loc[p.fs_perm_sec_id == stock_id][['price_date', 'close_price']]
                pseries = pseries.reset_index()
                pseries = pseries.set_index(keys=['price_date'])
                self.fast_prices.update({stock_id: pseries})

            pickle.dump(self.fast_prices, open(os.path.join(self._data_path, 'fast_prices.p'), 'wb'))

        return self.fast_prices

    def get_metadata_from_csv(self):
        # Get the universe data from file
        results = pd.read_csv(os.path.join(self._data_path, self._meta_file))
        results['stock_id'] = results['fs_perm_sec_id']
        self.metadata = results.set_index(['fs_perm_sec_id'])
        return self.metadata

    def get_universe_from_csv(self):
        # Get the universe data from file
        results = pd.read_csv(os.path.join(self._data_path, self._univ_file))
        results['date'] = pd.to_datetime(results['date'], format='%Y-%m-%d')
        results['index_date'] = results['date']
        results['stock_id'] = results['fs_perm_sec_id']
        self.universe = results.set_index(['fs_perm_sec_id', 'date'])
        return self.universe

    def get_factor_data_from_csv(self):
        # load the dividend data which is in a diff format
        ddata = pd.read_csv(os.path.join(self._data_path, self._dividend_file))
        ddata['ex_date'] = pd.to_datetime(ddata['ex_date'], format='%d/%m/%y')
        ddata['date'] = ddata['ex_date']
        ddata = ddata.set_index(['fs_perm_sec_id', 'date'])

        # merge the div data columns onto the universe
        result = pd.merge(self.universe, ddata, left_index=True, right_index=True, how='left')

        # then load and merge all the matrix style data from csv
        for frame, file in self.data_files.items():
            print('Loading: ', frame, file)
            tempdata = pd.read_csv(os.path.join(self._data_path, file))
            tempdata = pd.melt(tempdata, id_vars=['date'], value_vars=tempdata.columns.tolist()[1:])
            tempdata['date'] = pd.to_datetime(tempdata['date'], format='%Y-%m-%d')
            tempdata.columns = ['date', 'fs_perm_sec_id', frame]
            tempdata = tempdata.set_index(['fs_perm_sec_id', 'date'])
            result = pd.merge(result, tempdata, left_index=True, right_index=True, how='left')

        self.dataset = result
        return self.dataset

    def build_hist_data(self):
        if settings.USE_CACHE:
            self.meta_data = pd.read_pickle(os.path.join(self._data_path, 'meta_data.p'))
            self.universe = pd.read_pickle(os.path.join(self._data_path, 'univ_data.p'))
            self.dataset = pd.read_pickle(os.path.join(self._data_path, 'hist_data.p'))
        else:
            self.meta_data = self.get_metadata_from_csv()
            self.meta_data.to_pickle(os.path.join(self._data_path, 'meta_data.p'))
            self.universe = self.get_universe_from_csv()
            self.universe.to_pickle(os.path.join(self._data_path, 'univ_data.p'))
            self.dataset = self.get_factor_data_from_csv()
            self.dataset.to_pickle(os.path.join(self._data_path, 'hist_data.p'))

        # Add names to the hist_data
        self.dataset = self._merge_meta_data_to_hist()

        # Limit data for testing
        self.dataset = self.dataset.query('(date >= "' + settings.BACKTEST_START_DATE + '")')

        return self.dataset

    def get_prices_from_csv(self):
        # Load the data
        # then load and merge all the matrix style data from csv
        tempdata = pd.read_csv(os.path.join(self._data_path, self._price_file))
        # make it daily and then pad forward, this is needed for later on
        # idx = pd.date_range(min(p.price_date), max(p.price_date))
        # p.index = pd.DatetimeIndex(p.index)
        # p = p.reindex(idx, method='pad')

        tempdata = pd.melt(tempdata, id_vars=['date'], value_vars=tempdata.columns.tolist()[1:])
        tempdata['date'] = pd.to_datetime(tempdata['date'], format='%Y-%m-%d')
        tempdata.columns = ['date', 'fs_perm_sec_id', 'close_price']
        tempdata['stock_id'] = tempdata['fs_perm_sec_id']
        tempdata['price_date'] = tempdata['date']
        tempdata = tempdata.set_index(['fs_perm_sec_id', 'date'])
        # result = pd.merge(universe, tempdata, left_index=True, right_index=True, how='left')

        self.prices = tempdata
        return self.prices

    def _merge_meta_data_to_hist(self):
        self.dataset = self.dataset.reset_index()
        names = self.metadata
        names = names.reset_index()
        names = names[self._meta_cols]
        self.dataset = self.dataset.merge(names, how='left', on=['fs_perm_sec_id'])
        self.dataset = self.dataset.set_index(['fs_perm_sec_id', 'date'])
        return self.dataset

    def calc_factors(self):
        # pad some data and calculate some factors
        # TODO: calc historic annualised DY
        # calc rolling sum of div hist and then divide by price
        # self.dataset['ann_dps'] = self.dataset.groupby('stock_id')['div_adj'].rolling(2).sum()
        # self.dataset['ann_dps'] = self.dataset['ann_dps'].replace(0, np.nan)
        # self.dataset['ann_dps'] = self.dataset.ann_dps.fillna(method='pad')
        self.dataset['ann_dy'] = self.dataset['div_adj_ann'] / self.dataset['spo_adj_price']

        self.dataset['hist_dy'] = self.dataset['hist_dy'].replace(0, np.nan)
        self.dataset['hist_dy_series'] = self.dataset.hist_dy.fillna(method='pad')
        self.dataset['er'] = self.dataset['eps_fy1'] - self.dataset['eps_fy1'].shift(-365)
        self.dataset.franking_pct /= 100
        self.dataset['franking_pct_series'] = self.dataset.franking_pct.fillna(method='pad')
        self.dataset['gross_div'] = ((self.dataset.dps_cents / (
            1 - 0.30)) - self.dataset.dps_cents) * self.dataset.franking_pct

        self.dataset['forward_month_tret'] = self.dataset.groupby(level=0)['return_t'].shift(1)

        # get the tickers of stocks that pay divs
        divs = self.dataset.query('dps_cents > 0')
        div_stocks = divs.stock_id.unique().tolist()

        div_mask = self.dataset['stock_id'].isin(div_stocks)
        self.dataset.loc[div_mask, 'has_div'] = 1

        return self.dataset

    # def get_close_price(self, price_date, stock_id):
    #
    #     price_date = pd.to_datetime(price_date, format='%Y-%m-%d')
    #     close_price = np.nan
    #     day_count = 0
    #
    #     # if self.prices.query('(date == "' + price_date.strftime('%Y-%m-%d') + '") & (fs_perm_sec_id == "' + stock_id + '")')['close_price'].empty:
    #     #     while self.prices.query('(date == "' + price_date.strftime(
    #     #             '%Y-%m-%d') + '") & (fs_perm_sec_id == "' + stock_id + '")')['close_price'].empty and day_count <= 5:
    #     #         price_date += datetime.timedelta(days=1)
    #     #         day_count += 1
    #
    #     close_price = self.prices[(self.prices['stock_id'] == stock_id) & (self.prices['price_date'] == price_date.strftime('%Y-%m-%d'))]['close_price'][0]
    #
    #     close_price = self.prices.query('(date == "' + price_date.strftime('%Y-%m-%d') + '") & (fs_perm_sec_id == "' + stock_id + '")')['close_price'].values[0]
    #
    #     return close_price

    def get_price(self, price_date, stock_id):
        try:
            price = self.fast_prices[stock_id].loc[price_date].close_price[0]
        except:
            pdate = datetime.datetime.strptime(price_date, '%Y-%m-%d') - BDay(1)
            price = self.fast_prices[stock_id].loc[pdate].close_price
        return price
