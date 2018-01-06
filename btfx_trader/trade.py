import logging
from collections import defaultdict, OrderedDict, deque
from contextlib import suppress
from queue import Empty
from time import time, sleep
from threading import Thread

import requests
from btfxwss import BtfxWss

log = logging.getLogger(__name__)


def get_symbols():
    return list(
        map(lambda q: q.upper(),
            filter(lambda i: 'usd' in i,
                   requests.get('https://api.bitfinex.com/v1/symbols').json()))
    )


class Trader(Thread):
    _wss_client = BtfxWss
    _min_order_value = 35.
    _max_order_history = 100
    _sleep_time = 0.001
    _trade_types = {'market', 'limit'}

    def __init__(self, key, secret, symbols=None):
        super(Trader, self).__init__()
        if symbols:
            self.symbols = [s.upper() + ('' if 'usd' in s.lower() else 'USD') for s in symbols]
        else:
            self.symbols = get_symbols()

        self.connected = False
        self._prices = defaultdict(float)
        self._wallets = defaultdict(float)
        self._orders = OrderedDict()
        self._executed_orders = deque(maxlen=self._max_order_history)

        self._source_handlers = OrderedDict((
            ('orders', self._handle_order),
            ('orders_new', self._handle_order),
            ('orders_update', self._handle_order),
            ('orders_cancel', self._handle_order),
            ('wallets', self._handle_wallet)
        ))

        self.wss = self._wss_client(key=key, secret=secret, log_level='CRITICAL')
        self.start()

    def _connect(self):
        self.wss.start()
        log.debug('Waiting for btfxwss api socket connection...')
        while not self.wss.conn.connected.is_set():
            sleep(0.1)
        log.debug('Waiting for btfxwss api authentication...')
        self.wss.authenticate()
        log.debug('Subscribing to symbols...')
        for symbol in self.symbols:
            self.wss.subscribe_to_ticker(symbol)
        self.connected = True
        log.info('Successfully initialized')

    @staticmethod
    def _create_order_json(data):
        orders = []
        for _id, _, _, symbol, _, ts, remain_amount, amount, _, _, _, _, _, stat, _, _, price, exec_price, *_ in data:
            orders.append((_id, dict(
                symbol=symbol[1:].upper(),
                price=price,
                executed_price=exec_price,
                amount=amount,
                executed=amount - remain_amount,
                remaining=remain_amount,
                status=stat.split()[0],
                timestamp=ts / 1000,
            )))
        return orders

    @staticmethod
    def _log_order(_id, symbol, action, amount, price):
        log.info('%d (%6s) %-7s %-4s, %.8f for $%.2f at $%.2f', _id, symbol, action,
                 'BUY' if amount > 0 else 'SELL', abs(amount), abs(amount * price), price)

    def _handle_order(self, _type, data):
        if _type == 'os':
            orders = self._create_order_json(data)
            for _id, order in sorted(orders, key=lambda t: t[1]['timestamp']):
                self._orders[_id] = order
        else:
            _id, order = self._create_order_json([data])[0]
            if _type == 'oc':
                if order['status'].startswith('CANCEL'):
                    self._log_order(_id, order['symbol'], 'CANCEL', order['amount'], order['price'])
                else:
                    self._log_order(_id, order['symbol'], 'EXECUTE', order['executed'], order['executed_price'])
                    self._executed_orders.append((_id, order))
                del self._orders[_id]

            elif _type in {'on', 'ou'}:
                self._orders[_id] = order
                self._log_order(_id, order['symbol'], 'SUBMIT', order['amount'], order['price'])

    def _handle_wallet(self, _type, data):
        if _type == 'ws':
            for wallet_type, currency, balance, _, _ in data:
                if wallet_type.lower() == 'exchange':
                    self._wallets[currency.lower()] = float(balance)
        elif _type == 'wu':
            self._wallets[data[1].lower()] = float(data[2])

    def run(self):
        self._connect()
        while self.connected:
            start_loop = time()
            for source, handler in self._source_handlers.items():
                with suppress(Empty):
                    handler(*(getattr(self.wss, source).get_nowait()[0]))
            for symbol in self.symbols:
                with suppress(Empty):
                    data = self.wss.queue_processor.tickers[('ticker', symbol)].get_nowait()[0]
                    self._prices[symbol] = data[0][6]
            sleep(max(0, self._sleep_time - (time() - start_loop)))

    @property
    def prices(self):
        return self._prices.copy()

    @property
    def orders(self):
        return self._orders.copy()

    @property
    def executed_orders(self):
        return OrderedDict(self._executed_orders)

    @property
    def order_totals(self):
        totals = defaultdict(float)
        totals['usd'] = 0.
        for order in self.orders.values():
            if order['remaining'] < 0:
                totals[order['symbol']] += order['remaining']
            else:
                totals['usd'] += order['remaining'] * order['price']
        return totals

    @property
    def wallets(self):
        wallets = self._wallets.copy()
        for symbol, total in self.order_totals.items():
            wallets[symbol.replace('USD', '').lower()] -= abs(total)
        return wallets

    @property
    def orders_value(self):
        total = 0.
        for symbol, t in self.order_totals.items():
            total += abs(t) * (1 if t > 0 else self._prices[symbol])
        return round(total, 2)

    @property
    def value(self):
        total = 0.
        for c, amount in self._wallets.items():
            if c != 'usd':
                amount *= self._prices[c.upper() + 'USD']
            total += amount
        return round(total, 2)

    def cancel(self, _id):
        self.wss.cancel_order(multi=False, id=_id)

    def cancel_all(self, older_than=0):
        now = time()
        for _id, order in self._orders.items():
            if now - order['timestamp'] > older_than:
                self.cancel(_id)

    def order(self, symbol, price, *,
              ratio=None, dollar_amount=None,
              pad_price=None, trade_type='limit', return_id=True):

        assert ratio or dollar_amount, 'Must provide either `ratio` or `dollar_amount`'
        assert not (ratio and dollar_amount), 'Cannot provide both `ratio` and `dollar_amount`'
        assert trade_type.lower() in self._trade_types, \
            'Unknown trade type, try one of these: %s' % str(self._trade_types)[1:-1]

        buying = (ratio or dollar_amount) > 0
        symbol = symbol.upper() + ('USD' if 'usd' not in symbol.lower() else '')
        assert symbol in self.symbols, 'Trader not initialized with %s in symbols'

        if price == 'market' or trade_type == 'market':
            trade_type = 'market'
            price = self._prices[symbol]
            pad_price = 0.001

        if pad_price:
            delta = price * pad_price
            price += max(delta, 0.01) if buying else min(-delta, -0.01)

        assert price >= 0.01, 'Price cannot be less than $0.01'
        max_amount = self.wallets['usd'] / price if buying else self.wallets[symbol.lower().replace('usd', '')]
        if ratio:
            amount = round(max_amount * ratio, 8)
        else:
            amount = round(dollar_amount / price, 8)

        assertion_msg = 'Trade value (${:.2f}) is %s available trade value ($%.2f)'.format(abs(amount))
        assert abs(amount) >= self._min_order_value / price, assertion_msg % ('below minimum', self._min_order_value)
        assert abs(amount) <= max_amount, assertion_msg % ('above maximum', max_amount * price)

        current_order_ids = set(self._orders.keys())

        self.wss.new_order(
            cid=int(time()),
            type="EXCHANGE %s" % trade_type.upper(),
            symbol="t%s" % symbol,
            amount="%.8f" % amount,
            price="%.2f" % price,
        )

        while return_id:
            for _id in self._orders:
                if _id not in current_order_ids:
                    return _id
            else:
                sleep(self._sleep_time)

    def wait_execution(self, _id, seconds=1e9):
        start_time = time()
        while time() - start_time < seconds:
            try:
                ids, trades = tuple(zip(*self._executed_orders))
                return trades[ids.index(_id)]
            except ValueError:
                sleep(self._sleep_time)
        raise TimeoutError('Waiting for execution of order %d timed out after %d seconds' % (_id, seconds))

    def position(self, symbol):
        return (self._wallets[symbol.lower().replace('usd', '')] * self._prices[symbol]
                - self._wallets['usd']) / self.value

    def reset_position(self, symbol):
        try:
            _id = self.order(symbol, 'market', dollar_amount=self._wallets['usd'] - self.value / 2)
            self.wait_execution(_id)
        except AssertionError:
            log.error('Position already reset (%.4f)', self.position(symbol))

    def shutdown(self):
        self.connected = False
        for symbol in self.symbols:
            self.wss.unsubscribe_from_ticker(symbol)
        self.wss.stop()
        self.join()
