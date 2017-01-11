# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 13:09:36 2016

@author: Connah Cutbush (23/10/2016)

This creates a model for dividend harvest strategy using database 
data in Data folder.

- process the asx200 daily
- filter
    - ER > 0
    - DY high
    - volatiliy (avix > 27%)

"""
import datetime
import time

from data_access.utility import logging
from portfolio_tools.portfolio import Portfolio

if __name__ == '__main__':
    logger = logging.get_logger('port')
    now = datetime.datetime.now()
    start_time = time.time()
    print('--- Starting at %s ----' % datetime.datetime.now())
    port = Portfolio()
    # port.load()
    print('build trades')
    port.build_trades_list()
    print("--- %s seconds ---" % (time.time() - start_time))
    # port.save()
    print('export trades')
    start_time = time.time()
    port.trades_to_csv()
    print("--- %s seconds ---" % (time.time() - start_time))
    # print('export summary')
    print('build daily pnl')
    start_time = time.time()
    port.build_pnl()
    print("--- %s seconds ---" % (time.time() - start_time))
    # #port.save()
    print('export pnl')
    port.pnl_to_csv()
    port.summary()

    print('--- Done! at %s ----' % datetime.datetime.now())
