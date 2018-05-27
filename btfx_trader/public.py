from contextlib import suppress
from queue import Queue, Empty
import logging
import time

import requests
import btfxwss
import btfxwss.queue_processor
import btfxwss.connection

log = logging.getLogger(__name__)
# Multiprocessing queue is slow...
btfxwss.connection.Queue = btfxwss.queue_processor.Queue = Queue


def get_symbols():
    return list(
        map(lambda q: q.upper(),
            filter(lambda i: 'usd' in i,
                   requests.get('https://api.bitfinex.com/v1/symbols').json()))
    )


def get_symbols_as_updated(check_every=43200):
    symbols = set(get_symbols())
    for s in list(sorted(symbols)):
        yield s
    while True:
        time.sleep(check_every)
        current_symbols = set(get_symbols())
        new_symbols = current_symbols.difference(symbols)
        symbols.update(new_symbols)
        for s in list(sorted(new_symbols)):
            yield s


def _handle_ticker(data):
    return {
        'bid': data[0][0], 'bid_size': data[0][1],
        'ask': data[0][2], 'ask_size': data[0][3],
        'mid': round((data[0][0] + data[0][2]) / 2, 4),
        'weighted_mid': round((data[0][0] * data[0][1] +
                               data[0][2] * data[0][3]) /
                              (data[0][1] + data[0][3]), 4),
        'last_price': data[0][6],
        'spread': round(2 * (data[0][2] - data[0][0]) /
                        (data[0][0] + data[0][2]), 8),
        'change': data[0][5],
        'volume': data[0][7],
        'high': data[0][8],
        'low': data[0][9],
        'timestamp': round(time.time(), 0)
    }


def _handle_trade(data):
    if data[0] == 'tu':
        return {
            'id': data[1][0],
            'timestamp': data[1][1] / 1000,
            'price': data[1][3],
            'amount': data[1][2]
        }


def _handle_candle(data):
    return {
        'timestamp': data[0][0] / 1000,
        'open': data[0][1],
        'high': data[0][2],
        'low': data[0][3],
        'close': data[0][4],
        'volume': data[0][5]
    }


class PublicData:
    """
    Wrapper for BtfxWss for access to public bitfinex data
    :param types: list:
        (optional) data types to filter data by (tickers, trades, candles)
    :param symbols: list:
        (optional) symbol to filter data by (e.g. ['BTCUSD', 'ETHUSD'])
    """
    candle_periods = [
        '1m', '5m', '15m', '30m',
        '1h', '3h', '6h', '12h',
        '1D', '7D', '14D', '1M'
    ]

    def __init__(self, types=None, symbols=None):
        self.types = set(types or [])
        self.symbols = set(symbols or [])
        self._assert_types_are_correct(self.types)
        self.wss = btfxwss.BtfxWss(key='', secret='', log_level='CRITICAL')

    def __iter__(self):
        while True:
            types, symbols = self.types.copy(), self.symbols.copy()
            for _type in types:
                for symbol in symbols:
                    try:
                        yield _type, symbol, self.get_nowait(_type, symbol)
                    except Empty:
                        continue

    def get(self, _type, symbol):
        """
        Gets next available data (blocks until new datum is received)
        :param _type: str: type of data to get (e.g. 'tickers', 'candles'...)
        :param symbol: str:
            symbol for which to get data (e.g. 'BTCUSD', 'ETHUSD'...)
        :return: dict: requested data in json format
        :raises: AssertionError:
            if either of the provided type and symbol are unrecognized
        """
        assert _type in self.types, "Unknown type '%s'" % _type
        assert symbol in self.symbols, "Unknown symbol '%s'" % symbol
        while True:
            try:
                return self._get_nowait(_type, symbol)
            except Empty:
                time.sleep(1e-6)

    def get_nowait(self, _type, symbol):
        """
        Gets currently available data with no blocking
        :param _type: str: type of data to get (e.g. 'tickers', 'candles'...)
        :param symbol: str:
            symbol for which to get data (e.g. 'BTCUSD', 'ETHUSD'...)
        :return: dict:
            requested data in json format
        :raises:
            AssertionError:
                if either of the provided type and symbol are unrecognized
            Empty:
                if no new data is available (same as queue.Queue api)
        """
        assert _type in self.types, "Unknown type '%s'" % _type
        assert symbol in self.symbols, "Unknown symbol '%s'" % symbol
        return self._get_nowait(_type, symbol)

    def subscribe(self, _type, symbol):
        """
        Subscribe to a stream of data
        :param _type: str: type of data to get (e.g. 'tickers', 'candles'...)
        :param symbol: str:
            symbol for which to get data (e.g. 'BTCUSD', 'ETHUSD'...)
        """
        self._assert_types_are_correct([_type])
        self.types.add(_type)
        self.symbols.add(symbol)

        if _type == 'tickers':
            self.wss.subscribe_to_ticker(symbol)
        elif _type == 'trades':
            self.wss.subscribe_to_trades(symbol)
        elif isinstance(_type, tuple):
            self.wss.subscribe_to_candles(symbol, timeframe=_type[1])

    def connect(self):
        """Open a connection to a Bitfinex websocket"""
        self.wss.start()
        while not self.wss.conn.connected.is_set():
            time.sleep(1e-4)

        for _type in self.types:
            for symbol in self.symbols:
                self.subscribe(_type, symbol)

    def close(self):
        """Close the connection to the Bitfinex websocket"""
        for symbol in self.symbols:
            with suppress(KeyError):
                self.wss.unsubscribe_from_ticker(symbol)
            with suppress(KeyError):
                self.wss.unsubscribe_from_trades(symbol)
            with suppress(KeyError):
                self.wss.unsubscribe_from_candles(symbol)
        self.wss.stop()

    @staticmethod
    def _assert_types_are_correct(types):
        candle_types = []
        for _type in types:
            if isinstance(_type, str):
                assert _type in {'tickers', 'trades'}, \
                    "String type must be either 'tickers' or 'trades'"
            else:
                candle_types.append(_type)

        for name, period in candle_types:
            assert name == 'candles', \
                'Candles types must be in the format ("candles", <period>)'
            assert period in PublicData.candle_periods, \
                'Candle period must be one of %s' % PublicData.candle_periods

    @staticmethod
    def _clean_data(_type, data):
        result = data[0]
        if _type == 'trades':
            result = data[0] if isinstance(data[0][0], str) \
                else ['tu', data[0][0][0]]
        elif isinstance(_type, tuple):
            result = data[0] if isinstance(data[0][0][0], (int, float)) \
                else [data[0][0][0]]
        return result

    def _get_queue(self, _type, symbol):
        if _type == 'tickers':
            pair = ('ticker', symbol)
        elif _type == 'trades':
            pair = ('trades', symbol)
        else:
            pair = ('candles', symbol, _type[1])
            _type = 'candles'
        return getattr(self.wss.queue_processor, _type)[pair]

    def _get_nowait(self, _type, symbol):
        if _type == 'tickers':
            handler = _handle_ticker
        elif _type == 'trades':
            handler = _handle_trade
        else:
            handler = _handle_candle

        log.debug("Getting %s - %s from queues", str(_type), symbol)
        data = self._get_queue(_type, symbol).get_nowait()
        result = handler(self._clean_data(_type, data))
        if result is None:
            log.debug("%s - %s queue is empty", _type, symbol)
            raise Empty

        return result
