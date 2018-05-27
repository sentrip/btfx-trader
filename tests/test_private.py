from time import time, sleep
from threading import Thread
import pytest


# ========================== #
#       Account tests        #
# ========================== #

def test_connect_close(trader):
    trader.connect()
    sleep(1e-4)
    assert trader.wss.conn.connected.is_set(), \
        'Trader connection not set after connect'
    trader.subscribe('BTCUSD')
    assert 'BTCUSD' in trader.symbols, 'Did not add symbol on subscribe'
    trader.close()
    sleep(1e-4)
    assert trader._disconnect_event.is_set(), \
        'Trader did not set disconnect event on close'


def test_wallets_update(trader):
    # wallet init
    trader._update('ws', [
        ['EXCHANGE', 'usd', 100., None, None],
        ['EXCHANGE', 'btc', 10., None, None],
        ['EXCHANGE', 'eth', 5., None, None]
    ])
    sleep(0.002)  # wait for thread to update
    assert trader.wallets['usd'] == 100., 'Did not update usd in wallet'
    assert trader.wallets['btc'] == 10., 'Did not update btc in wallet'
    assert trader.wallets['eth'] == 5., 'Did not update eth in wallet'
    # wallet updates
    trader._update('wu', ['EXCHANGE', 'usd', 50., None, None])
    trader._update('wu', ['EXCHANGE', 'btc', 20., None, None])
    sleep(0.004)  # wait for thread to update
    assert trader.wallets['usd'] == 50., 'Did not update usd in wallet'
    assert trader.wallets['btc'] == 20., 'Did not update btc in wallet'
    assert trader.wallets['eth'] == 5., 'Did not update eth in wallet'


def test_orders_init_execute(trader):
    # orders init
    ts = time() * 1000
    expected_json_init = dict(
            symbol='BTCUSD', price=50., executed_price=45., amount=10.,
            executed=5., remaining=5., status='PARTIALLY', timestamp=ts / 1000,
        )
    trader._update('os', [
        [123, '', '', 'tBTCUSD', '', ts, 5., 10., '', '', '', '', '',  'PARTIALLY EXECUTED', '', '', 50., 45., '']
    ])
    sleep(0.002)  # wait for thread to update
    assert trader.orders[123] == expected_json_init, 'Incorrect order on init'
    assert trader.orders[123] == list(trader.orders.values())[-1], 'Incorrect latest order'
    assert len(trader.orders) == 1, 'Incorrect length of orders'
    # execute order
    ts = time() * 1000
    expected_json_execute = dict(
            symbol='BTCUSD', price=50., executed_price=43., amount=10.,
            executed=10., remaining=0., status='EXECUTED', timestamp=ts / 1000,
        )
    trader._update(
        'oc',
        [123, '', '', 'tBTCUSD', '', ts, 0., 10., '', '', '', '', '',  'EXECUTED @', '', '', 50., 43., '']
    )
    sleep(0.002)  # wait for thread to update
    assert 123 not in trader.orders, 'Did not remove executed order'
    assert len(trader.orders) == 0, 'Incorrect length of orders'
    assert list(trader.executed_orders.items())[-1] == (123, expected_json_execute), \
        'Incorrect order json added to executed orders'


def test_orders_update(trader, new_order, update_order):
    # new order
    ts = round(time(), 2)
    expected_json_new = dict(
            symbol='BTCUSD', price=50., executed_price=0., amount=10.,
            executed=0., remaining=10., status='ACTIVE', timestamp=ts,
        )
    _id = new_order(trader, 'BTCUSD', 50., 10., ts=ts)
    sleep(0.002)  # wait for thread to update
    assert trader.orders[_id] == expected_json_new, 'Incorrect new order'
    assert trader.orders[_id] == list(trader.orders.values())[-1], 'Incorrect latest order'
    assert len(trader.orders) == 1, 'Incorrect length of orders'
    # update order
    ts = round(time(), 2)
    expected_json_update = dict(
            symbol='BTCUSD', price=25., executed_price=0., amount=10.,
            executed=0., remaining=10., status='ACTIVE', timestamp=ts,
        )
    update_order(trader, _id, 25, 0., ts=ts)
    sleep(0.002)  # wait for thread to update
    assert trader.orders[_id] == expected_json_update, 'Incorrect update order'
    assert trader.orders[_id] == list(trader.orders.values())[-1], 'Incorrect latest order'
    assert len(trader.orders) == 1, 'Incorrect length of orders'
    # cancel order
    update_order(trader, _id, 25, 0, cancel=True)
    sleep(0.002)  # wait for thread to update
    assert _id not in trader.orders, 'Did not remove cancelled order'
    assert len(trader.orders) == 0, 'Incorrect length of orders'


def test_available_balances(trader, setup_trader, new_order, update_order):
    setup_trader(trader)
    trader._wallets['btc'] = 0.1
    trader._wallets['eth'] = 1.
    _id = new_order(trader, 'BTCUSD', 15000, 0.01)

    assert trader.available_balances['usd'] == 9850., \
        'Incorrect wallet value after buy BTCUSD'
    assert trader.available_balances['btc'] == 0.1, \
        'Incorrect wallet value after buy BTCUSD'

    update_order(trader, _id, 15000, 0.01, execute=True)

    assert trader.available_balances['usd'] == 9850., \
        'Incorrect wallet value after execute buy BTCUSD'
    assert trader.available_balances['btc'] == 0.11, \
        'Incorrect wallet value after execute buy BTCUSD'

    _id = new_order(trader, 'ETHUSD', 700, -1.)

    assert trader.available_balances['usd'] == 9850., \
        'Incorrect wallet value after sell ETHUSD'
    assert trader.available_balances['eth'] == 0., \
        'Incorrect wallet value after sell ETHUSD'

    update_order(trader, _id, 700, -1., execute=True)

    assert trader.available_balances['usd'] == 10550., \
        'Incorrect wallet value after execute sell BTCUSD'
    assert trader.available_balances['eth'] == 0., \
        'Incorrect wallet value after execute sell ETHUSD'


def test_position(trader, setup_trader):
    setup_trader(trader)
    assert trader.positions['BTCUSD'] == 0., 'Incorrect position with no coin'
    trader._wallets['usd'] -= 5000.
    trader._wallets['btc'] += 1 / 3
    assert trader.positions['BTCUSD'] == 0.5, 'Incorrect position with balanced dollars and coin'
    trader._wallets['usd'] -= 5000.
    trader._wallets['btc'] += 1 / 3
    assert trader.positions['BTCUSD'] == 1., 'Incorrect position with no dollars'
    trader._wallets['usd'] += 10000.
    trader._wallets['btc'] -= 2 / 3
    assert trader.positions['BTCUSD'] == 0., 'Incorrect position with imbalanced dollars and coin'


def test_value(trader, setup_trader, new_order, update_order):
    setup_trader(trader)
    assert trader.value == 10000., 'Incorrect initial value'
    _id = new_order(trader, 'BTCUSD', 15000, 0.01)
    assert trader.value == 10000., 'Incorrect value during trade'
    update_order(trader, _id, 15000, 0.01, execute=True)
    assert trader.value == 10000., 'Incorrect value after neutral trade'
    trader._prices['BTCUSD'] = 30000
    assert trader.value == 10150., 'Incorrect value after price increase'


def test_value_after_price_update(patched_get_symbols, trader, setup_trader):
    setup_trader(trader)
    trader._wallets['btc'] = 0.1
    trader.connect()
    _id = trader.order('BTCUSD', 15000, dollar_amount=150)
    value = trader.value
    trader.wss.queue_processor.account['Order Cancel'].put(
        [['oc', [_id, '', '', 'tBTCUSD', '', time() * 1000, 0, 0.01,
                 '', '', '', '', '', 'CANCELLED', '', '', 15000, 0, '']
          ], 0]
    )
    trader.wss.queue_processor.tickers[('ticker', 'BTCUSD')].put(
        [[[
            7500.0, 1., 7501.0, 1., -1.0, -0.01,
            7429.8, 26839.8, 7654.1, 7318.2
        ]], 1527273347.1]
    )
    now, diff = time(), 0
    while trader.value == value and diff < 1e-3:
        sleep(1e-5)
        diff = time() - now
    trader.close()
    assert trader.value != value, 'Did not update value in allotted time'


# ========================== #
#        Order tests         #
# ========================== #

def test_cancel_single(trader, setup_trader, new_order):
    setup_trader(trader)
    _id = new_order(trader, 'BTCUSD', 15000, 0.01)
    new_order(trader, 'BTCUSD', 15000, 0.01)
    assert len(trader.orders) == 2, 'Did not add both orders'
    trader.cancel(_id)
    assert len(trader.orders) == 1, 'Did not cancel order'


def test_cancel_all(trader, setup_trader, new_order):
    setup_trader(trader)
    now = time()
    for i in range(10):
        new_order(trader, 'BTCUSD', 15000, 0.01, ts=now - i * 10)
    assert len(trader.orders) == 10, 'Did not add all orders'
    trader.cancel_all(older_than=50)
    assert len(trader.orders) == 5, 'Did not cancel all orders'


def test_wait_execution(trader, setup_trader, new_order, update_order):
    setup_trader(trader)
    _id = new_order(trader, 'BTCUSD', 15000, 0.01)
    with pytest.raises(TimeoutError):
       trader.wait_execution(_id, seconds=0.001)
    del trader._orders[_id]
    _id = new_order(trader, 'BTCUSD', 15000, 0.01)
    result = []

    def wait(_idd, _wait):
        result.append(trader.wait_execution(_idd, seconds=_wait))
    t = Thread(target=wait, args=(_id, 0.1))
    t.start()
    update_order(trader, _id, 15150, 0.01, execute=True)
    t.join()
    assert len(trader.executed_orders) == 1, 'Did not execute order'
    assert list(trader.executed_orders.items())[-1] == (_id, result[0]), 'Incorrect items in orders'


def test_order_multiple_args_fails(trader, setup_trader):
    setup_trader(trader)
    with pytest.raises(AssertionError):
        trader.order("BTCUSD", 15000., dollar_amount=1000, ratio=0.5)


def test_order_dollar_amount(trader, setup_trader, update_order):
    setup_trader(trader)
    symbol, price = "BTCUSD", 15000.
    # buy
    _id = trader.order(symbol, price, dollar_amount=1000)
    assert round(trader.available_balances['usd'], 2) == 9000, \
        'Did not buy correct amount'
    amount = trader.orders[_id]['amount']
    update_order(trader, _id, price, amount, execute=True)
    assert round(trader.available_balances['btc'], 8) == amount, \
        'Did not buy correct amount'
    # sell
    _id = trader.order(symbol, price, dollar_amount=-1000)
    assert round(trader.available_balances['btc'], 8) == 0, \
        'Did not sell correct amount'
    amount = trader.orders[_id]['amount']
    update_order(trader, _id, price, amount, execute=True)
    assert round(trader.available_balances['usd'], 2) == 10000, \
        'Did not sell correct amount'


def test_order_ratio(trader, setup_trader, update_order):
    setup_trader(trader)
    symbol, price = "BTCUSD", 15000.
    # buy
    _id = trader.order(symbol, price, ratio=0.5)
    assert round(trader.available_balances['usd'], 2) == 5000, \
        'Did not buy correct amount'
    amount = trader.orders[_id]['amount']
    update_order(trader, _id, price, amount, execute=True)
    assert round(trader.available_balances['btc'], 8) == amount, \
        'Did not buy correct amount'
    # sell
    _id = trader.order(symbol, price, ratio=-0.5)
    assert round(trader.available_balances['btc'], 7) == round(amount / 2, 7), \
        'Did not sell correct amount'
    amount = trader.orders[_id]['amount']
    update_order(trader, _id, price, amount, execute=True)
    assert round(trader.available_balances['usd'], 2) == 7500, \
        'Did not sell correct amount'
    _id = trader.order(symbol, price, ratio=-1)
    assert round(trader.available_balances['btc'], 8) == 0, \
        'Did not sell correct amount'
    amount = trader.orders[_id]['amount']
    update_order(trader, _id, price, amount, execute=True)
    assert round(trader.available_balances['usd'], 2) == 10000, \
        'Did not sell correct amount'


def test_order_value_ratio(trader, setup_trader, update_order):
    setup_trader(trader)
    symbol, price = "BTCUSD", 15000.
    # buy
    _id = trader.order(symbol, price, value_ratio=0.5)
    assert round(trader.available_balances['usd'], 2) == 5000, \
        'Did not buy correct amount'
    amount = trader.orders[_id]['amount']
    update_order(trader, _id, price, amount, execute=True)
    assert round(trader.available_balances['btc'], 8) == amount, \
        'Did not buy correct amount'
    # sell
    _id = trader.order(symbol, price, value_ratio=-0.5)
    assert round(trader.available_balances['btc'], 8) == 0, \
        'Did not sell correct amount'
    amount = trader.orders[_id]['amount']
    update_order(trader, _id, price, amount, execute=True)
    assert round(trader.available_balances['usd'], 2) == 10000, \
        'Did not sell correct amount'


def test_order_market(trader, setup_trader, update_order):
    setup_trader(trader)
    # buy
    _id = trader.order("BTCUSD", 'market', dollar_amount=1000)
    assert round(trader.available_balances['usd'], 2) == 9000, \
        'Did not buy correct amount'
    amount = trader.orders[_id]['amount']
    update_order(trader, _id, 15000., amount, execute=True)
    assert round(trader.available_balances['btc'], 8) == amount, \
        'Did not buy correct amount'
    # sell
    _id = trader.order("BTCUSD", 'market', dollar_amount=-1000)
    assert round(trader.available_balances['btc'], 8) == 0, \
        'Did not sell correct amount'
    amount = trader.orders[_id]['amount']
    update_order(trader, _id, 15000., amount, execute=True)
    assert round(trader.available_balances['usd'], 2) == 10000, \
        'Did not sell correct amount'


def test_order_pad_price(trader, setup_trader, update_order):
    setup_trader(trader)
    symbol, price = "DSHUSD", 1000.
    # buy
    _id = trader.order(symbol, price, dollar_amount=1100, pad_price=0.1)
    assert round(trader.available_balances['usd'], 2) == 8900, \
        'Did not buy correct amount'
    amount = trader.orders[_id]['amount']
    update_order(trader, _id, trader.orders[_id]['price'], amount, execute=True)
    assert round(trader.available_balances['dsh'], 8) == amount, \
        'Did not buy correct amount'
    # sell
    _id = trader.order(symbol, price, dollar_amount=-900, pad_price=0.1)
    assert round(trader.available_balances['dsh'], 8) == 0, \
        'Did not sell correct amount'
    amount = trader.orders[_id]['amount']
    update_order(trader, _id, trader.orders[_id]['price'], amount, execute=True)
    assert round(trader.available_balances['usd'], 2) == 9800, \
        'Did not sell correct amount'


def test_order_id_with_previous_executed_orders(trader, setup_trader, update_order):
    setup_trader(trader)
    old_order = trader.wss.new_order
    trader._sleep_time = 1e-6

    def execute_on_order(**kwargs):
        def wrapped():
            sleep(1e-4)
            old_order(cid=int(time()), type='EXCHANGE LIMIT',
                      symbol='tBTCUSD', price=kwargs.get('price'),
                      amount=kwargs.get('amount'), id=123)
            update_order(trader, 123, 15000, 0.01, execute=True)
        Thread(target=wrapped).start()

    trader.wss.new_order = execute_on_order
    trader.order('BTCUSD', 15000, dollar_amount=300)
