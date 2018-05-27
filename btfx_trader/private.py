import logging
import time
from collections import deque, defaultdict, OrderedDict
from contextlib import suppress
from queue import Empty
from threading import Thread, Event

from btfxwss import BtfxWss
from .public import get_symbols_as_updated

log = logging.getLogger(__name__)


def _jsonify_orders(data):
    return [
        (datum[0],
         dict(
             symbol=datum[3][1:].upper(),
             price=datum[16],
             executed_price=datum[17],
             amount=datum[7],
             executed=datum[7] - datum[6],
             remaining=datum[6],
             status=datum[13].split()[0],
             timestamp=datum[5] / 1000
         )) for datum in data]


class _Account:
    """
    Class for parsing live web socket data
    from bitfinex into user-friendly interface
    """
    def __init__(self):
        self._handlers = {
            'o': self._handle_order,
            't': self._handle_ticker,
            'w': self._handle_wallet
        }
        self._executed_orders = deque(maxlen=100)
        self._orders = OrderedDict()
        self._prices = defaultdict(float)

        self._available_balances = defaultdict(float)
        self._wallets = defaultdict(float)

    @property
    def available_balances(self):
        """
        Amount of currency in each wallet currently available for trading
        :return: dict: available trading balances
        """
        balances = self._wallets.copy()
        for order in self._orders.values():
            if order['remaining'] < 0:
                symbol = order['symbol'].lower().replace('usd', '')
                balances[symbol] += order['remaining']
            else:
                balances['usd'] -= order['remaining'] * order['price']
        return defaultdict(float,
                           **{c: round(balances[c], 8) for c in balances})

    @property
    def executed_orders(self):
        """
        Sorted historical executed orders (earliest first) addressed by id
        :return: OrderedDict: orders executed since initialization
        """
        return OrderedDict(self._executed_orders)

    @property
    def orders(self):
        """
        Sorted currently active orders (earliest first) addressed by order id
        :return: OrderedDict: currently active orders
        """
        return self._orders

    @property
    def positions(self):
        """
        Ratio of total value invested in each symbol (excluding other symbols)
        :return: dict: symbol positions
        """
        return defaultdict(float, **{
            symbol: (self._wallets[symbol.lower().replace('usd', '')]
                     * self._prices[symbol]) / self.value
            for symbol in self._prices
        })

    @property
    def value(self):
        """
        Approximate value of all balances and investments
        :return: float: total value
        """
        total = 0.
        for currency, amount in self._wallets.items():
            if currency != 'usd':
                amount *= self._prices[currency.upper() + 'USD']
            total += amount
        return round(total, 2)

    @property
    def wallets(self):
        """
        Wallet balances for all symbols ('usd' is regular currency)
        :return: dict: wallet balances
        """
        return self._wallets

    @staticmethod
    def _log_order(action, _id, symbol, amount, price):
        log.info('%d (%6s) %-7s %-4s, %.8f for $%.2f at $%.2f',
                 _id, symbol, action, 'BUY' if amount > 0 else 'SELL',
                 abs(amount), abs(amount * price), price)

    def _handle_order_update(self, cmd, orders):
        for _id, order in sorted(orders, key=lambda t: t[1]['timestamp']):
            self._orders[_id] = order
            if cmd == 'on':
                self._log_order('SUBMIT', _id, order['symbol'],
                                order['amount'], order['price'])

    def _handle_order_close(self, orders):
        for _id, order in orders:
            if order['status'].startswith('CANCEL'):
                self._log_order('CANCEL', _id, order['symbol'],
                                order['amount'], order['price'])
            else:
                self._executed_orders.append((_id, order))
                self._log_order('EXECUTE', _id, order['symbol'],
                                order['executed'], order['executed_price'])
            if _id in self._orders:
                del self._orders[_id]

    def _handle_order(self, cmd, data):
        if cmd != 'os':
            data = [data]
        orders = _jsonify_orders(data)
        if cmd in {'os', 'on', 'ou'}:
            self._handle_order_update(cmd, orders)
        else:
            self._handle_order_close(orders)

    def _handle_ticker(self, cmd, data):
        symbol = cmd[1:]
        weighted_mid = (data[0][0] * data[0][1] +
                        data[0][2] * data[0][3]) / (data[0][1] + data[0][3])
        self._prices[symbol] = weighted_mid

    def _handle_wallet(self, cmd, data):
        if cmd == 'ws':
            for datum in data:
                if datum[0].lower() == 'exchange':
                    self._wallets[datum[1].lower()] = float(datum[2])
        elif cmd == 'wu':
            self._wallets[data[1].lower()] = float(data[2])

    def _update(self, cmd, data):
        self._handlers[cmd[0]](cmd, data)


class Trader(_Account):
    """
    Class interface for trading on the Bitfinex Exchange
    :param key: str: Bitfinex api-key
    :param secret:  str: Bitfinex api-secret
    """
    _sleep_time = 0.01
    _min_order_value = 35.
    _max_order_history = 100
    _trade_types = {'market', 'limit'}
    _sources = (
        'Orders', 'Order New', 'Order Update', 'Order Cancel', 'Wallets'
    )

    def __init__(self, key, secret):
        super(Trader, self).__init__()
        self.symbols = []
        self.symbols_gen = get_symbols_as_updated()
        self.wss = BtfxWss(key=key, secret=secret, log_level='CRITICAL')
        self._disconnect_event = Event()
        self._receiver = None

    def cancel(self, _id):
        """
        Cancel an order
        :param _id: int: order id
        :return: None
        """
        self.wss.cancel_order(multi=False, id=_id)

    def cancel_all(self, older_than=0):
        """
        Cancel all orders older than a certain time
        :param older_than: int/float:
            age of order that should be cancelled (in seconds)
        :return: None
        """
        now = time.time()
        for _id, order in self._orders.copy().items():
            if now - order['timestamp'] > older_than:
                self.cancel(_id)

    def connect(self):
        """Open a connection to a Bitfinex websocket"""
        self.wss.start()
        while not self.wss.conn.connected.is_set():
            time.sleep(1e-4)
        self.wss.authenticate()
        # This thread will wait for 99.999% of its life
        subscriber_thread = Thread(target=self._subscribe)
        subscriber_thread.setDaemon(True)
        subscriber_thread.start()
        # This thread should be working and is therefore joined
        self._receiver = Thread(target=self._receive)
        self._receiver.start()

    def close(self):
        """Close the connection to the Bitfinex websocket"""
        self._disconnect_event.set()
        for symbol in self.symbols:
            self.wss.unsubscribe_from_ticker(symbol)
        self.wss.stop()
        self._receiver.join()

    def order(self, symbol, price, *,
              dollar_amount=None, ratio=None, value_ratio=None,
              trade_type='limit', pad_price=None, return_id=True):
        """
        Make an exchange order
        :param symbol: str:
            ticker symbol to order (e.g. 'BTCUSD', 'ETHUSD'...)

        :param price: float:
            Price at which to submit order
            You can also pass "market" as an argument for price
            to order at current market value
            e.g. Trader.order('BTCUSD', 'market', dollar_amount=5)

        :param dollar_amount: float:
            Dollar equivalent value of trade amount (negative for selling)
            e.g. ordering 10 BTCUSD at $5 per coin
                -> Trader.order('BTCUSD', 5, dollar_amount=50)

        :param ratio: float:
            Ratio of available balance for requested symbol (negative for sell)
            e.g. With $1000 USD in wallet, ordering 100 BTCUSD at $5 per coin
                -> Trader.order('BTCUSD', 5, ratio=0.5)

        :param value_ratio: float:
            Ratio of Trader.value (negative for selling)
            e.g. With $500 in USD and $500 in BTCUSD (at $5) in wallet,
                 ordering 100 BTCUSD at $5 per coin
                -> Trader.order('BTCUSD', 5, value_ratio=0.5)

        :param trade_type: str: (optional)
            Bitfinex api trade type - one of {'market', 'limit'}
            see https://www.bitfinex.com/features for details

        :param pad_price: float: (optional)
            Ratio based price padding - used to undercut or overshoot price
            If buying then pad_price * price is added to submitted price
            If selling then pad_price * price is subtracted from price

        :param return_id: bool: (optional)
            If true, Trader.order blocks until order_id is returned
            from Bitfinex, otherwise returns immediately

        :return: int:
            If return_id is True, then order id is returned, otherwise None

        :raises: AssertionError:
            If values passed are non-consistent
        """

        assert dollar_amount or ratio or value_ratio, \
            'Must provide either `dollar_amount`, `ratio` or `value_ratio`'
        assert sum(bool(i) for i in [dollar_amount, ratio, value_ratio]) == 1,\
            'Must provide only 1 of `dollar_amount`, `ratio` or `value_ratio`'
        assert trade_type.lower() in self._trade_types, \
            'Unknown trade type, try one of these: %s' % str(self._trade_types)

        symbol_lower = symbol.lower().replace('usd', '')
        symbol = symbol.upper().split('USD')[0] + 'USD'
        buying = (dollar_amount or ratio or value_ratio) > 0

        if price == 'market' or trade_type == 'market':
            trade_type = 'market'
            price = self._prices[symbol]

        if pad_price:
            delta = price * pad_price
            price += max(delta, 0.01) if buying else min(-delta, -0.01)
        assert price >= 0.01, 'Price cannot be less than $0.01'

        if buying:
            max_amount = self.available_balances['usd'] / price
        else:
            max_amount = self.available_balances[symbol_lower]

        if dollar_amount:
            amount = dollar_amount / price
        elif ratio:
            amount = max_amount * ratio
        else:
            amount = min(max(-max_amount, self.value * value_ratio / price),
                         max_amount)
        amount, max_amount = round(amount, 8), round(max_amount, 8)

        assertion_msg = 'Trade value (${:.2f}) is %s available trade value ' \
                        '($%.2f)'.format(abs(amount * price))
        assert abs(amount) >= self._min_order_value / price, \
            assertion_msg % ('below minimum', self._min_order_value)
        assert abs(amount) <= max_amount, \
            assertion_msg % ('above maximum', max_amount * price)

        current_order_ids = set(self._orders.keys())
        if len(self._executed_orders) == 0:
            last_executed_id = None
        else:
            last_executed_id = self._executed_orders[-1][0]

        self.wss.new_order(
            cid=int(time.time()),
            type="EXCHANGE %s" % trade_type.upper(),
            symbol="t%s" % symbol,
            amount="%.8f" % amount,
            price="%.2f" % price,
        )

        while return_id:
            for _id in self._orders.copy().keys():
                # new order arrives in _orders
                if _id not in current_order_ids:
                    return _id
            else:
                # new order is executed immediately
                if len(self._executed_orders) > 0 \
                 and self._executed_orders[-1][0] != last_executed_id:
                    return self._executed_orders[-1][0]
                time.sleep(self._sleep_time)

    def subscribe(self, symbol):
        """
        Subscribe to a symbol for price watching
        :param symbol: str: symbol to subscribe to (e.g. 'BTCUSD', 'ETHUSD'...)
        """
        self.symbols.append(symbol)
        self.wss.subscribe_to_ticker(symbol)

    def wait_execution(self, _id, seconds=1e9):
        """
        Waits for an order to execute
        :param _id: int: id of order to wait for
        :param seconds: int/float: time to wait before raising error
        :return: dict: order in json format
        :raises: TimeoutError (if order is not executed in given time)
        """
        start_time = time.time()
        while time.time() - start_time < seconds:
            try:
                ids, trades = tuple(zip(*self._executed_orders))
                return trades[ids.index(_id)]
            except ValueError:
                time.sleep(self._sleep_time)
        raise TimeoutError('Waiting for execution of order '
                           '%d timed out after %d seconds' % (_id, seconds))

    def _receive(self):
        while not self._disconnect_event.is_set():
            for source in self._sources:
                with suppress(Empty):
                    q = self.wss.queue_processor.account[source]
                    cmd, data = q.get_nowait()[0]
                    self._update(cmd, data)

            for symbol in self.symbols:
                with suppress(Empty):
                    q = self.wss.queue_processor.tickers[('ticker', symbol)]
                    data = q.get_nowait()[0]
                    self._update('t' + symbol, data)

    def _subscribe(self):
        for symbol in self.symbols_gen:
            self.subscribe(symbol)
