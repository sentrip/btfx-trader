from queue import Empty
from threading import Timer
from time import sleep
import pytest
from btfx_trader import get_symbols_as_updated

NOWAIT_DATA = [
    (
        'tickers', ('ticker', 'BTCUSD'),
        [([[7500.0, 1., 7501.0, 1., -1.0, -0.01, 7429.8, 26839.8, 7654.1, 7318.2]], 1527273347.1)],
        ['bid'], [7500.0]
    ),
    (
        'trades', ('trades', 'BTCUSD'),
        [
            (['te', [250014166, 1527273332589, -0.5, 7500.0]], 1527273332.8),
            (['tu', [250014166, 1527273332589, -0.5, 7500.0]], 1527273332.8)
        ],
        ['price'], [7500.0]
    ),
    (
        'candles', ('candles', 'BTCUSD', '1m'),
        [([[1527273240000, 7500.0, 7500.8, 7500.0, 7500.5, 0.5]], 1527273240.0)],
        ['open'], [7500.0]
    )
]


def test_get_symbols_as_updated(patched_get_symbols):
    gen = iter(get_symbols_as_updated(check_every=0))
    for expected in ['BTCUSD', 'ETHUSD', 'LTCUSD', 'SANUSD']:
        symbol = next(gen)
        assert symbol == expected, 'Did not get correct symbol from gen'


def test_connect_close(data):
    data.connect()
    sleep(1e-4)
    assert data.wss.conn.connected.is_set(), \
        'Data connection not set after connect'
    data.close()


@pytest.mark.parametrize('name,pair,data_to_put,keys,values', NOWAIT_DATA)
def test_get_nowait(data, name, pair, data_to_put, keys, values):
    for d in data_to_put:
        getattr(data.wss.queue_processor, name)[pair].put_nowait(d)
    if name == 'candles':
        name = ('candles', '1m')
    try:
        d = data.get_nowait(name, 'BTCUSD')
        for k, v in zip(keys, values):
            assert d[k] == v, 'Did not get correct data for %s' % str(name)
    except Empty:
        assert name == 'trades', 'Error getting data'


@pytest.mark.parametrize('name,pair,data_to_put,keys,values', NOWAIT_DATA)
def test_get(data, name, pair, data_to_put, keys, values):
    def put(n, p, dd):
        for _d in dd:
            getattr(data.wss.queue_processor, n)[p].put(_d)

    Timer(1e-4, put, args=[name, pair, data_to_put]).start()
    if name == 'candles':
        name = ('candles', '1m')
    d = data.get(name, 'BTCUSD')
    for k, v in zip(keys, values):
        assert d[k] == v, 'Did not get correct data for %s' % str(name)


def test_iter(data):
    for name, pair, d, _, _ in NOWAIT_DATA:
        for _d in d:
            getattr(data.wss.queue_processor, name)[pair].put_nowait(_d)
    count = 0
    types = {'tickers', 'trades', ('candles', '1m')}
    for _type, symbol, d in data:
        assert symbol == 'BTCUSD', "Incorrect symbol for data iteration"
        assert _type in types, "Incorrect data type for iteration"
        count += 1
        if count >= 2:
            break
