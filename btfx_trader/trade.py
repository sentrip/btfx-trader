import logging
import time

from threading import Thread

from btfxwss import BtfxWss

log = logging.getLogger(__name__)


class Trader:
    def __init__(self, symbol, auth, total_balance_portion=0.2):
        self.symbol = symbol
        self.api = BtfxWss(*auth, log_level='CRITICAL')
        self.balance_portion = total_balance_portion
        self.balance, self.coin = self.update_wallet()
        self.price = 0.
        self.connected = False
        self.running = True
        self.updater = Thread(target=self.update_price)
        self.updater.start()

    def update_wallet(self):
        # todo get current total balance * self.balance_portion
        return 0, 0

    def update_price(self):
        self.api.start()
        log.debug('Waiting for btfxwss api socket connection...')
        while not self.api.conn.connected.is_set():
            time.sleep(0.1)
        log.debug('Subscribing to %s tickers', self.symbol)
        self.api.subscribe_to_ticker(self.symbol)
        while True:
            try:
                tickers = self.api.tickers(self.symbol)
                break
            except KeyError:
                continue
        log.debug("Connected! Now streaming latest prices for %s", self.symbol)
        self.connected = True
        while self.running:
            self.price = tickers.get()[0][0][6]

    def buy(self, percentage):
        if self.balance > 1:
            n = round(self.balance * percentage / self.price, 8)
            # todo execute trade
            # todo make sure price and executed price are the same
            self.balance -= self.price * n
            self.coin += n
            log.warning('Buying %s at %-.2f for %.1f!', self.symbol, self.price, n * self.price)
        else:
            log.error('Tried to buy when balance is less than $1, %f', self.balance)

    def sell(self, percentage):
        if self.coin > 0:
            n = round(self.coin * percentage, 8)
            # todo execute trade
            # todo make sure price and executed price are the same
            self.balance += self.price * n
            self.coin -= n
            log.warning('Selling %s at %-.2f for %.1f!', self.symbol, self.price, n * self.price)
        else:
            log.error('Tried to buy when coin is 0, %f', self.coin)

    def shutdown(self):
        self.api.unsubscribe_from_ticker(self.symbol)
        self.running = False
        self.api.stop()
        self.updater.join()
