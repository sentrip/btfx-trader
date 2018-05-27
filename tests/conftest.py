import os, sys
sys.path.append(os.path.abspath('../btfx_trader'))
from collections import defaultdict
from threading import Thread, Event
from time import time, sleep
from queue import Queue
import pytest
import random
import btfxwss.queue_processor
import requests
from btfx_trader import PublicData, Trader


class TestWSS:
    def __init__(self, key=None, secret=None, log_level=None, tr=None):
        self.trader = tr
        self.key = key
        self.secret = secret
        self.log_level = log_level
        ev = Event()
        Thread(target=lambda: (sleep(1e-3), ev.set())).start()
        self.conn = type('', (), {'connected': ev})
        self.queue_processor = btfxwss.queue_processor.QueueProcessor(
            data_q=Queue(), log_level='CRITICAL'
        )

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

    def subscribe_to_trades(self, trades):
        pass

    def unsubscribe_from_trades(self, trades):
        pass

    def subscribe_to_candles(self, candles, timeframe=None):
        pass

    def unsubscribe_from_candles(self, candles, timeframe=None):
        pass

    def new_order(self, **kwargs):
        self.trader._update('on',
              [kwargs.pop('id', None) or random.randint(1000000, 9999999),
               '', '', kwargs.get('symbol'),
               '', time() * 1000,
               float(kwargs.get('amount')), float(kwargs.get('amount')), '', '',
               '', '', '',
               'ACTIVE', '', '', float(kwargs.get('price')), 0., '']
        )

    def cancel_order(self, **kwargs):
        _id = kwargs.pop('id')
        order = self.trader.orders[_id]
        self.trader._update('oc', [
            _id, '', '', 't' + order['symbol'], '',
            order['timestamp'] * 1000, order['remaining'], order['amount'],
            '', '', '', '', '',  'CANCELLED', '', '',
            order['price'], order['executed_price'], ''
        ])


@pytest.fixture
def patched_get_symbols(monkeypatch):
    got_once = False
    first = b'["btcusd","ethusd","ltcusd","ethltc"]'
    second = b'["btcusd","ethusd","ltcusd","ethltc","sanusd"]'

    def fake_get(*args, **kwargs):
        nonlocal got_once
        if not got_once:
            got_once = True
            d = first
        else:
            d = second
        r = requests.Response()
        r.status_code, r._content = 200, d
        return r

    monkeypatch.setattr('requests.get', fake_get)
    yield


@pytest.fixture
def data():
    t = PublicData(
        types=['tickers', 'trades', ('candles', '1m')],
        symbols=['BTCUSD', 'LTCUSD', 'ETHUSD']
    )
    t.wss = TestWSS('test_key', 'test_secret', log_level='CRITICAL')
    return t


@pytest.fixture
def trader():
    t = Trader('test_key', 'test_secret')
    t.wss = TestWSS('test_key', 'test_secret', log_level='CRITICAL', tr=t)
    return t


@pytest.fixture
def setup_trader():
    def wrapped(t):
        _wallets = [['EXCHANGE', s.lower().replace('usd', ''),
                     0., None, None] for s in t.symbols]
        t._update('ws', [['EXCHANGE', 'usd', 10000., None, None]] + _wallets)
        t._prices = defaultdict(float)
        t._prices.update({
            'BTCUSD': 15000,
            'ETHUSD': 700,
            'LTCUSD': 300,
            'DSHUSD': 1000,
            'XRPUSD': 0.2
        })
    return wrapped


@pytest.fixture
def new_order():
    def wrapper(tr, symbol, price, amount, ts=None, _id=None):
        _id = _id or random.randint(1000000, 9999999)
        tr._update('on',
             [_id, '', '', 't'+symbol, '', (ts or time())*1000, amount, amount,
              '', '', '', '', '',  'ACTIVE', '', '', price, 0., '']
        )
        return _id
    return wrapper


@pytest.fixture
def update_order():
    # This is bad but the alternative is a mock Bitfinex trading platform so...
    def wrapper(tr, _id, price, amount, cancel=False, execute=False, ts=None):
        if execute or cancel:
            cmd = 'oc'
            if execute:
                status = 'EXECUTED @ ($%.2f)' % price
            else:
                status = 'CANCELLED'
        else:
            cmd = 'ou'
            if amount != 0:
                status = 'PARTIALLY EXECUTED'
            else:
                status = 'ACTIVE'

        exc_price = tr._orders[_id]['executed_price']
        if amount != 0:
            if exc_price != 0:
                exc_price = (exc_price + price) / 2

            s = tr._orders[_id]['symbol'].lower().replace('usd', '')
            dollars = tr._wallets['usd'] - amount * price
            tr._update('wu', ['EXCHANGE', 'usd', dollars, None, None])
            tr._update('wu', [
                'EXCHANGE', s, tr._wallets[s] + tr._orders[_id]['executed'] + amount, None, None
            ])

        tr._update(cmd, [_id, '', '', 't' + tr._orders[_id]['symbol'], '',
                         (ts or time()) * 1000,
                         tr._orders[_id]['remaining'] - amount,
                         tr._orders[_id]['amount'],
                         '', '', '', '', '',  status, '', '',
                         price, exc_price, ''])
    return wrapper
