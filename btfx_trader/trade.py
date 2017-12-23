import logging
import time

from threading import Thread

from btfxwss import BtfxWss
from bitex import Bitfinex
import requests.exceptions

log = logging.getLogger(__name__)


class Trader:
    def __init__(self, symbol, auth):
        key, secret = auth
        self.symbol = symbol
        self.api = BtfxWss(key=key, secret=secret, log_level='CRITICAL')
        self.rest = Bitfinex(key=key, secret=secret)
        self.price = 0.
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
            data = tickers.get()[0][0]
            price = data[6]
            self.price = price
            log.debug('Updated price - %.2f, %s', price, str(data))

    def safe_query(self, path, **kwargs):
        while True:
            start = time.time()

            try:
                resp = self.rest.private_query(path, **kwargs)
                if resp.status_code == 200:
                    return resp
                else:
                    log.error('%s query (%s) returned error status %d, %s', path, self.symbol, resp.status_code, resp.text)
            except Exception as e:
                log.error('Error (%s) requesting %s: %s', self.symbol, path, repr(e))

            time.sleep(max(0, 1 - (time.time() - start)))

    def wallet(self):
        symbol = self.symbol.lower().strip('usd')
        balance, coin = None, None
        while balance is None or coin is None:
            resp = self.safe_query('balances').json()
            for i in resp:
                if i['type'] == 'exchange':
                    if i['currency'] == 'usd':
                        balance = eval(i['available'])
                    elif i['currency'] == symbol:
                        coin = eval(i['available'])
        return balance, coin

    def _order(self, order, use_all=False, force_price=None):
        price = force_price or self.price
        while True:
            balance, coin = self.wallet()

            if order > 0:
                side = 'buy'
                amount = balance / price
            else:
                side = 'sell'
                amount = coin

            if not round(amount, 8) > 0:
                log.error('Tried to %s %s when %s is 0, %f', side, self.symbol, 'balance' if order > 0 else 'coin',
                          coin)
                return

            q = {
                'symbol': self.symbol,
                'amount': '%.8f' % (amount * abs(order)),
                'side': side,
                'type': 'exchange limit',
                'price': '%.2f' % (price * (0.995 if side == 'sell' else 1.005)),
                'use_all_available': int(use_all)
                 }
            try:
                resp = self.rest.private_query('order/new', params=q)
                if resp.status_code != 200:
                    raise ConnectionError
                return resp
            except requests.exceptions.ReadTimeout:
                log.error('Timeout when to %s %s when %s is 0, %f',
                          side, self.symbol, 'balance' if order > 0 else 'coin', coin)
                return
            except Exception as e:
                log.error('ORDER ERROR (%s): %s', self.symbol, repr(e))

    def _wait_execution(self, _id):
        exec_price = 0.

        while exec_price == 0.:
            start = time.time()

            status = self.safe_query('order/status', params={'order_id': _id}).json()
            if not status['is_live']:
                exec_price = float(status['avg_execution_price'])

            time.sleep(max(0, 1 - (time.time() - start)))
        return exec_price

    def order(self, order, use_all=False, force_price=None):
        order = min(max(-1., order), 1.)
        resp = self._order(order, use_all=use_all, force_price=force_price)
        if resp is None:
            log.warning('%s %s at %-.2f!',  'Bought' if order > 0 else 'Sold', self.symbol, self.price)
        else:
            j = resp.json()
            price = self._wait_execution(j['order_id'])
            original_amount = eval(j['original_amount'])
            log.warning('%s %s at %-.2f for $%.1f!', 'Bought' if order > 0 else 'Sold',
                        self.symbol, price, original_amount * price)
            return price

    def shutdown(self):
        self.api.unsubscribe_from_ticker(self.symbol)
        self.running = False
        self.api.stop()
        self.updater.join()


# class Trader:
#     def __init__(self, symbol, auth):
#         key, secret = auth
#         self.symbol = symbol
#         self.api = BtfxWss(key=key, secret=secret, log_level='CRITICAL')
#         self.rest = Bitfinex(key=key, secret=secret)
#         self.price = None
#         self.connected = False
#         self.running = True
#         self.updater = Thread(target=self.update_price)
#         self.updater.start()
#         while self.price is None:
#             time.sleep(0.5)
#
#     def update_price(self):
#         self.api.start()
#         log.debug('Waiting for btfxwss api socket connection...')
#         while not self.api.conn.connected.is_set():
#             time.sleep(0.1)
#         log.debug('Subscribing to %s tickers', self.symbol)
#         self.api.subscribe_to_ticker(self.symbol)
#         while True:
#             try:
#                 tickers = self.api.tickers(self.symbol)
#                 break
#             except KeyError:
#                 continue
#         log.debug("Connected! Now streaming latest prices for %s", self.symbol)
#         self.connected = True
#         while self.running:
#             self.price = tickers.get()[0][0][6]
#
#     def wallet(self, symbol):
#         if symbol != 'usd':
#             symbol = symbol.lower().strip('usd')
#         res = None
#         while res is None:
#             resp = self.rest.private_query('balances').json()
#             for i in resp:
#                 if i['type'] == 'exchange' and i['currency'] == symbol:
#                     res = eval(i['available'])
#         return res
#
#     def _order(self, symbol, amount, side):
#         q = {'symbol': symbol, 'amount': amount, 'side': side,
#              'type': 'exchange market', 'price': '100000.0'}
#         return self.rest.private_query('order/new', params=q)
#
#     def buy(self, percentage):
#         percentage = min(percentage, 1)
#         while True:
#             try:
#                 balance, coin = self.wallet('usd'), self.wallet(self.symbol)
#                 if balance > 1:
#                     n = round(balance * percentage / self.price, 8)
#                     resp = self._order(self.symbol, '%.8f' % n, 'buy')
#                     if resp.status_code != 200:
#                         log.error('Buy order for %s returned error %d, %s', self.symbol, resp.status_code, resp.text)
#                     else:
#                         j = resp.json()
#                         log.warning('Bought %s at %-.2f for $%.1f!',
#                                     self.symbol, eval(j['price']), eval(j['original_amount']) * eval(j['price']))
#                 else:
#                     log.error('Tried to buy %s when balance is less than $1, %f', self.symbol, balance)
#                 return
#             except Exception as e:
#                 log.error('Error in %s: %s', self.symbol, repr(e))
#
#     def sell(self, percentage):
#         percentage = min(percentage, 1)
#         while True:
#             try:
#                 balance, coin = self.wallet('usd'), self.wallet(self.symbol)
#                 if coin > 0:
#                     n = round(coin * percentage, 8)
#                     resp = self._order(self.symbol, '%.8f' % n, 'sell')
#                     if resp.status_code != 200:
#                         log.error('Sell order for %s returned error %d, %s', self.symbol, resp.status_code, resp.text)
#                     else:
#                         j = resp.json()
#                         log.warning('Sold %s at %-.2f for $%.1f!',
#                                     self.symbol, eval(j['price']), eval(j['original_amount']) * eval(j['price']))
#                 else:
#                     log.error('Tried to sell %s when coin is 0, %f', self.symbol, coin)
#                 return
#             except Exception as e:
#                 log.error('Error in %s: %s', self.symbol, repr(e))
#
#     def shutdown(self):
#         self.api.unsubscribe_from_ticker(self.symbol)
#         self.running = False
#         self.api.stop()
#         self.updater.join()
