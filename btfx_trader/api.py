import logging
import time
from collections import deque, defaultdict
from datetime import datetime
from functools import partial
from queue import Queue

import btfxwss

log = logging.getLogger(__name__)


def parse_tickers(symbol, data):
    items, timestamp = data
    d = dict(zip(['bid', 'bid_size', 'ask', 'ask_size', 'change',
                  'change_perc', 'last_price', 'volume',  'high', 'low'],
                 items[0]))
    d.update(time=datetime.fromtimestamp(timestamp), symbol=symbol)
    return [d]


def parse_set(keys, symbol, data):
    items, timestamp = data
    ts = datetime.fromtimestamp(timestamp)
    result = []
    items = items[0] if isinstance(items[0][0], list) else [items[0]]
    for set_of_items in items:
        d = dict(symbol=symbol, time=ts)
        d.update(dict(zip(keys, set_of_items)))
        result.append(d)
    return result


def parse_candles(symbol, data):
    items, timestamp = data
    result = []
    items = items[0] if isinstance(items[0][0], list) else [items[0]]
    for set_of_items in reversed(items):
        d = dict(symbol=symbol, time=datetime.fromtimestamp(set_of_items[0] / 1000))
        d.update(dict(zip(['open', 'high', 'low', 'close', 'volume'], set_of_items[1:])))
        result.append(d)
    return result


def parse_trades(symbol, data):
    items, _ = data
    if len(items) == 1:
        result = []
        for trade in items[0]:
            ts = datetime.fromtimestamp(trade.pop(1) / 1000)
            d = dict(zip(['id', 'amount', 'price'], trade))
            d.update(time=ts, symbol=symbol)
            result.append(d)
        return result
    else:
        symbol, trade = items
        if symbol == 'tu':
            ts = datetime.fromtimestamp(trade.pop(1) / 1000)
            d = dict(zip(['id', 'amount', 'price'], trade))
            d.update(time=ts, symbol=symbol)
            return [d]
        else:
            return []


class CoinAPI:
    """
    tickers     - BID, BID_SIZE, ASK, ASK_SIZE, DAILY_CHANGE, DAILY_CHANGE_PERC, LAST_PRICE, VOLUME, HIGH, LOW
    books       - PRICE, SIG_FIG, VOLUME
    raw_books   - ID, PRICE, AMOUNT
    trades      - ID, TIMESTAMP, AMOUNT, PRICE
    candles     - OPEN, HIGH, LOW, CLOSE, VOLUME
    """
    data_types = ['tickers', 'books', 'raw_books', 'trades', 'candles']
    parsers = {
        'tickers': parse_tickers,
        'books': partial(parse_set, ['price', 'sig_fig', 'volume']),
        'raw_books': partial(parse_set, ['id', 'price', 'amount']),
        'trades': parse_trades,
        'candles': parse_candles
    }

    def __init__(self, symbols=("BTCUSD",), data_types=None):
        self.data_types = data_types or self.data_types
        self.symbols = symbols
        self.connected = False

        self.api = btfxwss.BtfxWss('', '', log_level='CRITICAL')
        self.data = {k: {c: None for c in self.data_types} for k in symbols}
        self.return_queues = {k: {c: Queue() for c in self.data_types} for k in symbols}
        self.seen = defaultdict(lambda: deque(maxlen=5))

    def connect(self):
        """Connects to btfxwss api and subscribes to queues for each symbol in self.symbols"""
        self.api.start()
        log.debug('Waiting for btfxwss api socket connection...')
        while not self.api.conn.connected.is_set():
            time.sleep(0.1)
        log.debug('Subscribing to symbols...')
        for symbol in self.symbols:
            if 'tickers' in self.data_types:
                self.api.subscribe_to_ticker(symbol)
            if 'books' in self.data_types:
                self.api.subscribe_to_order_book(symbol)
            if 'raw_books' in self.data_types:
                self.api.subscribe_to_raw_order_book(symbol)
            if 'trades' in self.data_types:
                self.api.subscribe_to_trades(symbol)
            if 'candles' in self.data_types:
                self.api.subscribe_to_candles(symbol)
        self.wait_for_queues()
        self.connected = True
        log.info('Successfully initialized')

    def wait_for_queues(self):
        """Blocks until there is at least one data point in all queues set up in connect method"""
        log.debug('Waiting on connections of symbols and queue setup...')
        loaded_symbols = []
        start = time.time()
        while len(loaded_symbols) != len(self.symbols):
            for symbol in self.symbols:
                for data_type in self.data_types:
                    if symbol not in loaded_symbols:
                        try:
                            self.data[symbol][data_type] = getattr(self.api, data_type)(symbol)
                            if all(self.data[symbol][t] is not None for t in self.data_types):
                                loaded_symbols.append(symbol)
                        except KeyError:
                            continue
            if time.time() - start >= 10:
                log.info('Currently connected: %-2d / %-2d  -  not connected: %s',
                         len(loaded_symbols), len(self.symbols),
                         ', '.join(map(lambda s: s.strip('USD'),
                                       sorted(list(set(self.symbols) - set(loaded_symbols))))))
                start = time.time()
        log.debug('Done setting up queues, waiting on queues to all have data...')
        while any(any(q.empty() for q in self.data[symbol].values()) for symbol in self.symbols):
            time.sleep(0.01)

    def shutdown(self):
        """Un-subscribes from all queues and shuts down btfxwss api"""
        log.debug('Un-subscribing from all symbols...')
        for symbol in self.symbols:
            if 'tickers' in self.data_types:
                self.api.unsubscribe_from_ticker(symbol)
            if 'books' in self.data_types:
                self.api.unsubscribe_from_order_book(symbol)
            if 'raw_books' in self.data_types:
                self.api.unsubscribe_from_raw_order_book(symbol)
            if 'trades' in self.data_types:
                self.api.unsubscribe_from_trades(symbol)
            if 'candles' in self.data_types:
                self.api.unsubscribe_from_candles(symbol)
        log.debug('Shutting down api socket...')
        self.api.stop()
        self.connected = False
        log.info('Successfully shut down')

    def filter_candles(self, cleaned):
        _cleaned = []
        for i in cleaned:
            t = i.pop('time')
            if i not in self.seen[i['symbol']]:
                self.seen[i['symbol']].append(i)
                i['time'] = t
                _cleaned.append(i)
            else:
                log.debug('Filtering seen item %s', str(i))
        return _cleaned

    def get(self, symbol, _type):
        data = self.data[symbol][_type].get()
        cleaned = self.parsers[_type](symbol, data)
        if _type == 'candles':
            cleaned = self.filter_candles(cleaned)
        for datum in cleaned:
            self.return_queues[symbol][_type].put(datum)
        return self.return_queues[symbol][_type].get()
