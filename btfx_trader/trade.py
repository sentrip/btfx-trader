import logging
from collections import defaultdict
from contextlib import suppress
from time import sleep

from bitex import Bitfinex
from requests.exceptions import ReadTimeout

log = logging.getLogger(__name__)


class Trader:
    _n_retry_attempts = 5
    _api = Bitfinex  # for testing purposes

    def __init__(self, key, secret):
        self.api = self._api(key=key, secret=secret)
        self._wallet = defaultdict(float)
        self.cached_wallet = defaultdict(float)
        self.traded_since_update = True

    def query(self, path, **kwargs):
        wait_time = 0.3
        for attempt in range(1, 1 + self._n_retry_attempts):
            try:
                log.debug('Requesting (%d): %s with params: %s', attempt, path, str(kwargs))
                resp = self.api.private_query(path, **kwargs)
                resp.raise_for_status()
                return resp
            except ReadTimeout as e:
                log.error('REQUEST TIMEOUT (%d): %s - %s', attempt, path, repr(e))
                if path == 'order/new':
                    return None  # Timeout on order submission does not mean order failure
            except Exception as e:
                log.error('REQUEST ERROR (%d): %s - %s', attempt, path, repr(e))
                sleep(wait_time)
                wait_time *= 2
        else:
            log_str = 'MAX REQUESTS REACHED (%d): %s' % (self._n_retry_attempts, path)
            with suppress(NameError, AttributeError):
                log_str += ' - last response [%d]: %s' % (resp.status_code, resp.text)
            log.error(log_str)
            raise ConnectionError('Max retries exceeded (%d)' % self._n_retry_attempts)

    def active_orders(self):
        resp = self.query('orders')
        return [] if resp is None else resp.json()

    def last_order(self):
        orders = self.active_orders()
        if len(orders) > 0:
            return list(sorted(orders, key=lambda k: float(k['timestamp']), reverse=True)).pop(0)

    def cancel(self, _id):
        self.query('order/cancel', params={'id': _id})

    def cancel_all(self):
        self.query('order/cancel/all')

    def wallet(self):
        resp = self.query('balances')
        all_wallets = [] if resp is None else resp.json()
        wallet = defaultdict(float)
        _wallet = {w['currency']: float(w['available']) for w in all_wallets if w['type'] == 'exchange'}
        wallet.update(_wallet)
        self.cached_wallet = wallet
        return wallet

    def update_wallet(self):
        if self.traded_since_update:
            resp = self.query('balances')
            all_wallets = [] if resp is None else resp.json()
            self._wallet = defaultdict(float)
            _wallet = {w['currency']: float(w['available']) for w in all_wallets if w['type'] == 'exchange'}
            self._wallet.update(_wallet)
            self.cached_wallet = self._wallet
            self.traded_since_update = False

    def order(self, symbol, price, *,
              percentage=None, dollar_amount=None,
              pad_price=None, wait_execution=True):

        assert percentage or dollar_amount, 'Must provide either `percentage` or `dollar_amount`'
        assert not (percentage and dollar_amount), 'Cannot provide both `percentage` and `dollar_amount`'

        buying = (percentage or dollar_amount) > 0
        market_order = False
        if price == 'market':
            resp = self.api.query('GET', 'pubticker/%s' % symbol).json()
            price = float(resp['last_price'])
            pad_price = 0.001
            market_order = True

        if pad_price:
            delta = price * pad_price
            price += max(delta, 0.01) if buying else min(-delta, -0.01)

        self.update_wallet()
        self.traded_since_update = True
        balance = self._wallet['usd']
        coin = self._wallet[symbol.lower().replace('usd', '')]

        max_amount = balance / price if buying else coin
        if percentage:
            amount = round(max_amount * abs(percentage), 8)
        else:
            amount = round(abs(dollar_amount) / price, 8)

        assertion_msg = 'Trade value ($%.2f) is %s available trade value ($%.2f)'
        assert amount <= max_amount, assertion_msg % (amount * price, 'above maximum', max_amount * price)
        assert amount >= 30 / price, assertion_msg % (amount * price, 'below minimum', 30.)

        resp = self.query('order/new', params={
            'symbol': symbol,
            'amount': '%.8f' % amount,
            'side': 'buy' if buying else 'sell',
            'type': 'exchange %s' % ('market' if market_order else 'limit'),
            'price': '%.2f' % round(price, 2)
        })

        execution_price, execution_amount = price, amount
        if wait_execution:
            if resp:
                _id = resp.json()['order_id']
            else:
                _id = (self.last_order() or {'id': None})['id']

            if _id:
                execution_price, execution_amount = self._wait_order(_id)

        log.warning('%s %s at %-.2f for $%.2f!', 'Bought' if buying else 'Sold',
                    symbol, execution_price, execution_amount * price)
        return execution_price

    def _wait_order(self, _id,):
        while True:
            resp = self.api.private_query('order/status', params={'order_id': _id})
            if resp.status_code == 200:
                j = resp.json()
                if not j['is_live']:
                    return float(j['avg_execution_price']), float(j['executed_amount'])
                else:
                    executed_percentage = float(j['executed_amount']) / float(j['original_amount'])
                    log.debug('Order %s FOUND and still live (%.2f%s)', str(_id), executed_percentage * 100, '%')
                    sleep(1)

            elif resp.status_code == 429:
                log.debug('Order %s status request RATE LIMITED', str(_id))
                sleep(15)

            else:
                log.debug('Order %s NOT FOUND', str(_id))
                sleep(2.5)

    def reset(self, symbol):
        self.update_wallet()
        self.traded_since_update = True
        balance = self._wallet['usd']
        coin = self._wallet[symbol.lower().replace('usd', '')]
        price = float(self.api.query('GET', 'pubticker/%s' % symbol).json()['last_price'])
        value = balance + coin * price
        expected_value = value / 2
        to_spend = balance - expected_value
        assert abs(to_spend) > 30, 'Values are close enough'
        self.order(symbol, 'market', dollar_amount=to_spend)
