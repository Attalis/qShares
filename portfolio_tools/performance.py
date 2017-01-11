__author__ = 'connahcutbush'


class Position:
    def __init__(self, portfolio_id, date, asset_id, shares, price):
        self.portfolio_id = portfolio_id
        self.date = date
        self.asset_id = asset_id
        self.shares = shares
        self.price = price

    def value(self):
        return self._shares * self._price


class ProfitAndLoss:
    def __init__(self, stock_id):
        self.daily_pnl = []

    def append(self, position):
        self.daily_pnl.append(position)

    def value_at_date(self, trade_date):

        for position in self.daily_pnl:
            if position.date == trade_date:
                return position

    def fill_forward(self, new_date, fill_prices=False):

        pass


class Statistics:
    def __init__(self):
        pass
