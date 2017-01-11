__author__ = 'connahcutbush'

import settings


class Blotter(object):
    def __init__(self):
        self.trade_list = {}


class Trade(object):
    def __init__(self, stock_id, exchange_ticker, open_date, start_price, logger=None):
        # generic trade fields
        self.stock_id = stock_id
        self.exchange_ticker = exchange_ticker
        self.open_date = open_date
        self.start_price = start_price
        self.close_date = ''
        self.end_price = 0.0
        self.trade_size = 0.0
        self.days_open = 0
        self.direction = ''
        self.status = 'open'

        # div specific fields
        self.div_ex_date = ''
        self.div_pershare = 0.0  # in dollars
        self.franking_pct = 0.0  # eg 100%

        self.dy = 0.0
        self.trade_size_adj = 0.0

    def shares(self):
        if self.direction == 'short':
            shares = -(self.trade_size / self.start_price)
        else:
            shares = self.trade_size / self.start_price

        return shares

    def close(self, date, close_price):
        self.status = 'closed'
        self.close_date = date
        self.end_price = close_price

    def rollover(self):
        self.days_open += 1
        print(self.days_open)

    def cash_dividend(self):
        if self.direction == 'short':
            cdiv = -(self.shares() * self.div_pershare)
        else:
            cdiv = self.shares() * self.div_pershare

        return cdiv

    def tran_cost(self):
        return -(self.shares() * settings.TRADE_COMMISSION)

    def fund_cost(self):
        fcost = self.trade_size * (self.days_open / 365) * (settings.BBSW + settings.LONG_FUNDING_COST_BBSW)
        if self.direction == 'long':
            fcost = -(fcost)
        return fcost

    def franking_credit(self):
        if self.direction == 'short':
            fcredit = 0
        else:
            fcredit = self.shares() * self._calc_franking_amount(self.div_pershare, self.franking_pct)

        return fcredit

    def franking_pershare(self):
        frnk = self._calc_franking_amount(self.div_pershare, self.franking_pct)
        return frnk

    @staticmethod
    def _calc_franking_amount(div_amount, franking_perc):
        # reference: http://frankingcredits.com.au/franking-credit-formulas/
        # Franking Credits = ((Dividend Amount / (1-Company Tax Rate)) – Dividend Amount ) * Franking percent)
        franking = ((div_amount / (1 - settings.COMPANY_TAX_RATE)) - div_amount) * franking_perc
        return franking

    # @staticmethod
    # def gross_dividend(div_amount, franking_amount):
    #     gross_div = div_amount + franking_amount
    #     return (gross_div)

    def short_cost(self):
        if self.direction == 'short':
            scost = -(self.trade_size * (settings.SHORT_COST * self.days_open / 365))
        else:
            scost = 0

        return scost

    def entry_price(self):
        if self.direction == 'short':
            ep = +(self.shares() * self.start_price)
        else:
            ep = -(self.shares() * self.start_price)
        return ep

    def exit_price(self):
        if self.direction == 'short':
            xp = -(self.shares() * self.start_price)
        else:
            xp = +(self.shares() * self.start_price)
        return xp

    def cash_at_end(self):
        cash = self.tran_cost() + self.fund_cost() + self.cash_dividend() + self.exit_price()
        return cash

    def current_return(self, price_date, close_price):
        position_value = close_price * self.shares()
        interim_return = (position_value / self.entry_price()) - 1
        # reinvest the div on the ex date for long trade legs
        if price_date == self.ex_date and self.direction == 'long':
            position_value += (self.div_pershare * self.shares())
        return interim_return
