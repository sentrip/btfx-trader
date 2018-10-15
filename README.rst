===========
btfx-trader
===========


.. image:: https://img.shields.io/pypi/v/btfx_trader.svg
    :target: https://pypi.python.org/pypi/btfx-trader/

.. image:: https://travis-ci.org/sentrip/btfx-trader.svg?branch=master
    :target: https://travis-ci.com/sentrip/btfx-trader/

.. image:: https://readthedocs.org/projects/btfx-trader/badge/?version=latest
    :target: https://btfx-trader.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://codecov.io/gh/sentrip/btfx-trader/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/sentrip/btfx-trader

.. image:: https://pyup.io/repos/github/sentrip/btfx_trader/shield.svg
    :target: https://pyup.io/repos/github/sentrip/btfx-trader/
    :alt: Updates



Simple to use wrappers for Bitfinex's web socket api


* Free software: GNU General Public License v3
* Documentation: https://btfx-trader.readthedocs.io.


Features
--------

* Access to cryptocurrency data with an API similar to queue.Queue
* Simple trading API that responds to account data

Installation
-------------

To install btfx-trader, run this command in your terminal:

.. code-block:: shell

    pip install btfx-trader

Usage
------

To use public data:

.. code-block:: python

    from btfx_trader import PublicData

    q = PublicData(types=['tickers'], symbols=['BTCUSD'])
    q.connect()

    while True:
        data = q.get('tickers', 'BTCUSD')


To make an order:

.. code-block:: python

    from btfx_trader import Trader

    trader = Trader('YOUR_BITFINEX_KEY', 'YOUR_BITFINEX_SECRET')
    trader.connect()
    # Order 0.01 BTC at $10000 per bitcoin
    order_id = trader.order('BTCUSD', 10000, dollar_amount=100)


To cancel an order:

.. code-block:: python

    # for a single order
    trader.cancel(order_id)
    # for multiple orders
    trader.cancel_all(older_than=10)



Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
