=====
Usage
=====


Public Data
------------

To use public data provided by Bitfinex in your project, do::

    from btfx_trader import PublicData

    q = PublicData(types=['tickers'], symbols=['BTCUSD'])
    q.connect()

    while True:
        data = q.get('tickers', 'BTCUSD')
::

To use the non-blocking api::

    from queue import Empty
    while True:
        try:
            data = q.get_nowait('tickers', 'BTCUSD')
        except Empty:
            continue
::


Private Data
-------------

To use the trading api in your project, do::

    from btfx_trader import Trader
    trader = Trader('YOUR_BITFINEX_KEY', 'YOUR_BITFINEX_SECRET')
    trader.connect()
::


To make an order::

    # order 0.01 BTC at $10000/BTC
    _id = trader.order('BTCUSD', 10000, dollar_amount=100)
    print(trader.orders[_id])
::

To cancel an order::

    # for a single order
    trader.cancel(_id)
    # for multiple orders
    trader.cancel_all(older_than=10)
