import random
import unittest
from collections import defaultdict
from multiprocessing import Queue
from threading import Thread
from time import sleep, time

from btfx_trader.trade import Trader


class FakeWSS:
    def __init__(self, key=None, secret=None, log_level=None):
        self.key = key
        self.secret = secret
        self.log_level = log_level

        self.conn = type('', (), {'connected': type('', (), {'is_set': lambda: True})})
        self.queue_processor = type('', (), {'tickers': defaultdict(Queue)})

        self.orders = Queue()
        self.orders_new = Queue()
        self.orders_update = Queue()
        self.orders_cancel = Queue()
        self.wallets = Queue()

    def start(self):
        pass

    def stop(self):
        pass

    def authenticate(self):
        pass

    def subscribe_to_ticker(self, ticker):
        pass

    def unsubscribe_from_ticker(self, ticker):
        pass

    def new_order(self, **kwargs):
        self.orders_new.put(
            [('on',
              [random.randint(1000000, 9999999), '', '',  kwargs.get('symbol'), '', time() * 1000,
               float(kwargs.get('amount')), float(kwargs.get('amount')),  '', '', '', '', '',
               'ACTIVE', '', '', float(kwargs.get('price')), 0., '']), 0]
        )


class FakeTrader(Trader):
    _wss_client = FakeWSS


class TestTraderWSSUpdates(unittest.TestCase):
    def setUp(self):
        self.trader = FakeTrader('key', 'secret', symbols=['BTCUSD'])

    def tearDown(self):
        self.trader.shutdown()

    def test_init(self):
        pass  # Tests initialization of fake api

    def test_wallets_updates(self):
        # wallet init
        data = [('ws', [
            ['EXCHANGE', 'usd', 100., None, None],
            ['EXCHANGE', 'btc', 10., None, None],
            ['EXCHANGE', 'eth', 5., None, None]
        ]), 0]
        self.trader.wss.wallets.put(data)
        sleep(0.002)  # wait for thread to update
        self.assertEqual(100., self.trader.wallets['usd'], 'Did not update usd in wallet')
        self.assertEqual(10., self.trader.wallets['btc'], 'Did not update btc in wallet')
        self.assertEqual(5., self.trader.wallets['eth'], 'Did not update eth in wallet')
        # wallet updates
        self.trader.wss.wallets.put([('wu', ['EXCHANGE', 'usd', 50., None, None]), 0])
        self.trader.wss.wallets.put([('wu', ['EXCHANGE', 'btc', 20., None, None]), 0])
        sleep(0.004)  # wait for thread to update
        self.assertEqual(50., self.trader.wallets['usd'], 'Did not update usd in wallet')
        self.assertEqual(20., self.trader.wallets['btc'], 'Did not update btc in wallet')
        self.assertEqual(5., self.trader.wallets['eth'], 'Did not update eth in wallet')

    def test_orders_init_execute(self):
        # orders init
        ts = time() * 1000
        data = [('os', [
            [123, '', '', 'tBTCUSD', '', ts, 5., 10., '', '', '', '', '',  'PARTIALLY EXECUTED', '', '', 50., 45., '']
        ]), 0]
        expected_json_init = dict(
                symbol='BTCUSD', price=50., executed_price=45., amount=10.,
                executed=5., remaining=5., status='PARTIALLY', timestamp=ts / 1000,
            )
        self.trader.wss.orders.put(data)
        sleep(0.002)  # wait for thread to update
        self.assertEqual(expected_json_init, self.trader.orders[123], 'Incorrect order on init')
        self.assertEqual(list(self.trader.orders.values())[-1], self.trader.orders[123], 'Incorrect latest order')
        self.assertEqual(1, len(self.trader.orders), 'Incorrect length of orders')
        # execute order
        ts = time() * 1000
        data = [('oc',
                 [123, '', '', 'tBTCUSD', '', ts, 0., 10., '', '', '', '', '',  'EXECUTED @', '', '', 50., 43., '']), 0]
        expected_json_execute = dict(
                symbol='BTCUSD', price=50., executed_price=43., amount=10.,
                executed=10., remaining=0., status='EXECUTED', timestamp=ts / 1000,
            )
        self.trader.wss.orders.put(data)
        sleep(0.002)  # wait for thread to update
        assert 123 not in self.trader.orders, 'Did not remove executed order'
        self.assertEqual(0, len(self.trader.orders), 'Incorrect length of orders')
        self.assertEqual((123, expected_json_execute), list(self.trader.executed_orders.items())[-1],
                         'Incorrect order json added to executed orders')

    def test_orders_updates(self):
        # new order
        ts = time() * 1000
        data = [('on',
                 [321, '', '', 'tBTCUSD', '', ts, 10., 10., '', '', '', '', '',  'ACTIVE', '', '', 50., 0., '']), 0]
        expected_json_new = dict(
                symbol='BTCUSD', price=50., executed_price=0., amount=10.,
                executed=0., remaining=10., status='ACTIVE', timestamp=ts / 1000,
            )
        self.trader.wss.orders.put(data)
        sleep(0.002)  # wait for thread to update
        self.assertEqual(expected_json_new, self.trader.orders[321], 'Incorrect new order')
        self.assertEqual(list(self.trader.orders.values())[-1], self.trader.orders[321], 'Incorrect latest order')
        self.assertEqual(1, len(self.trader.orders), 'Incorrect length of orders')
        # update order
        ts = time() * 1000
        data = [('ou',
                 [321, '', '', 'tBTCUSD', '', ts, 10., 10., '', '', '', '', '',  'ACTIVE', '', '', 25., 0., '']), 0]
        expected_json_update = dict(
                symbol='BTCUSD', price=25., executed_price=0., amount=10.,
                executed=0., remaining=10., status='ACTIVE', timestamp=ts / 1000,
            )
        self.trader.wss.orders.put(data)
        sleep(0.002)  # wait for thread to update
        self.assertEqual(expected_json_update, self.trader.orders[321], 'Incorrect update order')
        self.assertEqual(list(self.trader.orders.values())[-1], self.trader.orders[321], 'Incorrect latest order')
        self.assertEqual(1, len(self.trader.orders), 'Incorrect length of orders')
        # cancel order
        ts = time() * 1000
        data = [('oc',
                 [321, '', '', 'tBTCUSD', '', ts, 10., 10., '', '', '', '', '',  'CANCELLED', '', '', 25., 0., '']), 0]
        self.trader.wss.orders.put(data)
        sleep(0.002)  # wait for thread to update
        assert 321 not in self.trader.orders, 'Did not remove cancelled order'
        self.assertEqual(0, len(self.trader.orders), 'Incorrect length of orders')


class TestTrader(unittest.TestCase):
    def setUp(self):
        self.trader = FakeTrader('key', 'secret', symbols=['BTCUSD', 'ETHUSD', 'LTCUSD', 'DSHUSD', "XRPUSD"])
        _wallets = [['EXCHANGE', s.lower().replace('usd', ''), 0., None, None] for s in self.trader.symbols]
        data = [('ws', [['EXCHANGE', 'usd', 10000., None, None]] + _wallets), 0]
        self.trader.wss.wallets.put(data)
        self.trader._prices = defaultdict(float)
        self.trader._prices.update({
            'BTCUSD': 15000,
            'ETHUSD': 700,
            'LTCUSD': 300,
            'DSHUSD': 1000,
            'XRPUSD': 0.2
        })
        sleep(0.005)

    def tearDown(self):
        self.trader.shutdown()

    def _new_order(self, symbol, price, amount):
        _id = random.randint(1000000, 9999999)
        self.trader._orders[_id] = dict(
                symbol=symbol, price=price, executed_price=0., amount=amount,
                executed=0., remaining=amount, status='ACTIVE', timestamp=time() / 1000,
            )
        return _id

    def _update_order(self, _id, price, amount, execute=False):
        if execute:
            status = 'EXECUTED @ ($%.2f)' % price
        else:
            status = 'PARTIALLY EXECUTED'

        self.trader._orders[_id].update(dict(
                executed_price=price, timestamp=time() / 1000, status=status,
                executed=amount, remaining=self.trader._orders[_id]['amount'] - amount
            ))
        if execute:
            self.trader._executed_orders.append((_id, self.trader._orders[_id]))
            del self.trader._orders[_id]

    def test_order_totals(self):
        self._new_order('BTCUSD', 15000, 0.01)
        self.assertEqual(150., self.trader.order_totals['usd'], 'Incorrect total for buying BTCUSD')
        self._new_order('ETHUSD', 700, 0.1)
        self.assertEqual(220., self.trader.order_totals['usd'], 'Incorrect total for buying ETHUSD, BTCUSD')
        _id = self._new_order('BTCUSD', 15000, -0.005)
        self.assertEqual(220., self.trader.order_totals['usd'], 'Incorrect total for selling BTCUSD')
        self.assertEqual(-0.005, self.trader.order_totals['BTCUSD'], 'Incorrect total for selling BTCUSD')
        self._new_order('BTCUSD', 15000, 0.1)
        self._update_order(_id, 15000, -0.0025)
        self.assertEqual(1720., self.trader.order_totals['usd'], 'Incorrect total for updating sell BTCUSD')
        self.assertEqual(-0.0025, self.trader.order_totals['BTCUSD'], 'Incorrect total for updating sell BTCUSD')
        self._update_order(_id, 15000, -0.005, execute=True)
        self.assertEqual(1720., self.trader.order_totals['usd'], 'Incorrect total for executing sell BTCUSD')
        self.assertEqual(0., self.trader.order_totals['BTCUSD'], 'Incorrect total for updating sell BTCUSD')

    def test_wallets(self):
        self.trader._wallets['btc'] = 0.1
        self.trader._wallets['eth'] = 1.
        _id = self._new_order('BTCUSD', 15000, 0.01)
        self.assertEqual(9850., self.trader.wallets['usd'], 'Incorrect wallet value after buy BTCUSD')
        self.assertEqual(0.1, self.trader.wallets['btc'], 'Incorrect wallet value after buy BTCUSD')
        self._update_order(_id, 15000, 0.01, execute=True)
        self.trader._wallets['usd'] -= 150
        self.trader._wallets['btc'] += 0.01
        self.assertEqual(9850., self.trader.wallets['usd'], 'Incorrect wallet value after execute buy BTCUSD')
        self.assertEqual(0.11, self.trader.wallets['btc'], 'Incorrect wallet value after execute buy BTCUSD')

        _id = self._new_order('ETHUSD', 700, -1.)
        self.assertEqual(9850., self.trader.wallets['usd'], 'Incorrect wallet value after sell ETHUSD')
        self.assertEqual(0., self.trader.wallets['eth'], 'Incorrect wallet value after sell ETHUSD')
        self._update_order(_id, 700, -1., execute=True)
        self.trader._wallets['usd'] += 700
        self.trader._wallets['eth'] -= 1
        self.assertEqual(10550., self.trader.wallets['usd'], 'Incorrect wallet value after execute sell BTCUSD')
        self.assertEqual(0., self.trader.wallets['eth'], 'Incorrect wallet value after execute sell ETHUSD')

    def test_orders_value(self):
        self.assertEqual(0., self.trader.orders_value, 'Incorrect total value with no orders')
        self._new_order('BTCUSD', 15000, 0.01)
        self.assertEqual(150., self.trader.orders_value, 'Incorrect total value for buying BTCUSD')
        self._new_order('ETHUSD', 700, 0.1)
        self.assertEqual(220., self.trader.orders_value, 'Incorrect total value for buying ETHUSD, BTCUSD')
        _id = self._new_order('BTCUSD', 15000, -0.01)
        self.assertEqual(370., self.trader.orders_value, 'Incorrect total value for selling BTCUSD')
        self._update_order(_id, 15150, -0.01, execute=True)
        self.assertEqual(220., self.trader.orders_value, 'Incorrect total value for executing sell BTCUSD')

    def test_value(self):
        self.assertEqual(10000., self.trader.value, 'Incorrect initial value')
        _id = self._new_order('BTCUSD', 15000, 0.01)
        self.assertEqual(10000., self.trader.value, 'Incorrect value during trade')
        self._update_order(_id, 15000, 0.01, execute=True)
        self.trader._wallets['usd'] -= 150
        self.trader._wallets['btc'] += 0.01
        self.assertEqual(10000., self.trader.value, 'Incorrect value after neutral trade')
        self.trader._prices['BTCUSD'] = 30000
        self.assertEqual(10150., self.trader.value, 'Incorrect value after price increase')

    def test_wait_execution(self):
        _id = self._new_order('BTCUSD', 15000, 0.01)
        with self.assertRaises(TimeoutError):
            self.trader.wait_execution(_id, seconds=0.001)
        del self.trader._orders[_id]
        _id = self._new_order('BTCUSD', 15000, 0.01)
        result = []
        t = Thread(target=lambda _idd, _wait: result.append(self.trader.wait_execution(_idd, seconds=_wait)),
                   args=(_id, 0.1))
        t.start()
        self._update_order(_id, 15150, 0.01, execute=True)
        t.join()
        self.assertEqual(1, len(self.trader.executed_orders))
        self.assertEqual((_id, result[0]), list(self.trader.executed_orders.items())[-1])

    def test_position(self):
        self.assertEqual(-1., self.trader.position('BTCUSD'), 'Incorrect position with no coin')
        self.trader._wallets['usd'] -= 5000.
        self.trader._wallets['btc'] += 1 / 3
        self.assertEqual(0., self.trader.position('BTCUSD'), 'Incorrect position with balanced dollars and coin')
        self.trader._wallets['usd'] -= 5000.
        self.trader._wallets['btc'] += 1 / 3
        self.assertEqual(1., self.trader.position('BTCUSD'), 'Incorrect position with no dollars')
        self.trader._wallets['usd'] += 2500.
        self.trader._wallets['btc'] -= 1 / 6
        self.assertEqual(0.5, self.trader.position('BTCUSD'), 'Incorrect position with imbalanced dollars and coin')
        self.trader._wallets['usd'] += 5000.
        self.trader._wallets['btc'] -= 1 / 3
        self.assertEqual(-0.5, self.trader.position('BTCUSD'), 'Incorrect position with imbalanced dollars and coin')

    def test_order_incorrect_params(self):
        with self.assertRaises(AssertionError):
            self.trader.order('BTCUSD', 1.)
        with self.assertRaises(AssertionError):
            self.trader.order('BTCUSD', 1., ratio=1, dollar_amount=1)
        with self.assertRaises(AssertionError):
            self.trader.order('BTCUSD', 0.)
        with self.assertRaises(AssertionError):
            self.trader.order('BTCUSD', 15000., ratio=1.5)
        with self.assertRaises(AssertionError):
            self.trader.order('BTCUSD', 15000., dollar_amount=15000)  # balance is 10000
        with self.assertRaises(AssertionError):
            self.trader.order('BTCUSD', 15000., dollar_amount=20)  # min is 35
        with self.assertRaises(AssertionError):
            self.trader.order('BTCUSD', 15000., dollar_amount=20, trade_type='ASDFDSF')

    def test_order_correct_params(self):
        self.trader._wallets['usd'] = 15000
        self.trader._wallets['btc'] = 1.
        self.trader._wallets['xrp'] = 75000.
        _id = self.trader.order('BTCUSD', 15000., ratio=1)
        self.assertEqual(1., self.trader.orders[_id]['amount'], 'Incorrect order amount for ratio(+)')
        _id = self.trader.order('BTCUSD', 15000., ratio=-1)
        self.assertEqual(-1., self.trader.orders[_id]['amount'], 'Incorrect order amount for ratio(-)')
        self.trader._orders.clear()
        _id = self.trader.order('BTCUSD', 15000., dollar_amount=150)
        self.assertEqual(0.01, self.trader.orders[_id]['amount'], 'Incorrect order amount for dollar amount(+)')
        _id = self.trader.order('BTCUSD', 15000., dollar_amount=-150)
        self.assertEqual(-0.01, self.trader.orders[_id]['amount'], 'Incorrect order amount for dollar amount(-)')
        self.trader._orders.clear()
        _id = self.trader.order('BTCUSD', 15000 / 1.01, ratio=1, pad_price=0.01)
        self.assertEqual(1, self.trader.orders[_id]['amount'], 'Incorrect order amount for pad price(+)')
        _id = self.trader.order('BTCUSD', 15000 / 1.01, ratio=-1, pad_price=0.01)
        self.assertEqual(-1, self.trader.orders[_id]['amount'], 'Incorrect order amount for pad price(-)')
        self.trader._orders.clear()
        _id = self.trader.order('XRPUSD', 0.19, ratio=1, pad_price=0.001)
        self.assertEqual(75000., self.trader.orders[_id]['amount'], 'Incorrect order amount for min pad price(+)')
        _id = self.trader.order('XRPUSD', 0.21, ratio=-1, pad_price=0.001)
        self.assertEqual(-75000., self.trader.orders[_id]['amount'], 'Incorrect order amount for min pad price(-)')
