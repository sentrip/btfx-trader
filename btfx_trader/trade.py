import logging
import time

from threading import Thread

from btfxwss import BtfxWss
from bitex import Bitfinex

log = logging.getLogger(__name__)


class Trader:
    def __init__(self, symbol, auth):
        self.symbol = symbol
        self.api = BtfxWss(*auth, log_level='CRITICAL')
        self.rest = Bitfinex(*auth)
        self.price = None
        self.connected = False
        self.running = True
        self.updater = Thread(target=self.update_price)
        self.updater.start()
        while self.price is None:
            time.sleep(0.5)

    def update_price(self):
        self.api.start()
        log.debug('Waiting for btfxwss api socket connection...')
        while not self.api.conn.connected.is_set():
            time.sleep(0.1)
        log.debug('Subscribing to %s tickers', self.symbol)
        self.api.subscribe_to_ticker(self.symbol)
        while True:
            try:
                tickers = self.api.tickers(self.symbol)
                break
            except KeyError:
                continue
        log.debug("Connected! Now streaming latest prices for %s", self.symbol)
        self.connected = True
        while self.running:
            self.price = tickers.get()[0][0][6]

    def wallet(self, symbol):
        if symbol != 'usd':
            symbol = symbol.lower().strip('usd')
        res = None
        while res is None:
            resp = self.rest.private_query('balances').json()
            for i in resp:
                if i['type'] == 'exchange' and i['currency'] == symbol:
                    res = eval(i['available'])
        return res

    def _order(self, symbol, amount, side):
        q = {'symbol': symbol, 'amount': amount, 'side': side,
             'type': 'exchange market', 'price': '100000.0'}
        return self.rest.private_query('order/new', params=q)

    def buy(self, percentage):
        percentage = min(percentage, 1)
        balance, coin = self.wallet('usd'), self.wallet(self.symbol)
        if balance > 1:
            n = round(balance * percentage / self.price, 8)
            resp = self._order(self.symbol, '%.8f' % n, 'buy')
            if resp.status_code != 200:
                log.error('Buy order returned error %d, %s', resp.status_code, resp.text)
            else:
                j = resp.json()
                log.warning('Bought %s at %-.2f for $%.1f!',
                            self.symbol, eval(j['price']), eval(j['original_amount']) * eval(j['price']))
        else:
            log.error('Tried to buy when balance is less than $1, %f', balance)

    def sell(self, percentage):
        percentage = min(percentage, 1)
        balance, coin = self.wallet('usd'), self.wallet(self.symbol)
        if coin > 0:
            n = round(coin * percentage, 8)
            resp = self._order(self.symbol, '%.8f' % n, 'sell')
            if resp.status_code != 200:
                log.error('Sell order returned error %d, %s', resp.status_code, resp.text)
            else:
                j = resp.json()
                log.warning('Sold %s at %-.2f for $%.1f!',
                            self.symbol, eval(j['price']), eval(j['original_amount']) * eval(j['price']))
        else:
            log.error('Tried to buy when coin is 0, %f', coin)

    def shutdown(self):
        self.api.unsubscribe_from_ticker(self.symbol)
        self.running = False
        self.api.stop()
        self.updater.join()
