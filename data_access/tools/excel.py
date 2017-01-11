__author__ = 'Connah Cutbush'

import pandas as pd


class Builder(object):
    def get_date_list(self, start_date, end_date, freq):
        f = 'D'
        if freq == 'D':
            f = 'B'
        elif freq == 'M':
            f = 'BM'
        return pd.bdate_range(start_date, end_date, f).tolist()

    def extract_data(self, data_file):
        xl = pd.ExcelFile(data_file)
        df = xl.parse('Sheet', skiprows=3, index_col='Date', na_values=['NA'], parse_dates=True)
        df.index.name = 'Date'
        df = df[pd.notnull(df.index)]
        return df
