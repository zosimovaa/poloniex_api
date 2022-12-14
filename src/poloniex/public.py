import requests
import datetime
import pytz

import logging
logger = logging.getLogger(__name__)


class PublicAPIError(Exception):
    """Ошибка при обращении к публичному API"""

    def __init__(self, message, method, body=None):
        self.message = message
        self.method = method
        self.body = body
        super().__init__(self.message)

    def __str__(self):
        message = "{0}. Method: {1}".format(self.message, self.method)
        if self.body is not None:
            message += ". Response: {0})".format(self.body)
        return message


class PublicAPI:
    """Класс реализует работу с публичным API poloniex.com"""
    PUBLIC_API_URL = "https://poloniex.com/public?command="
    PERIODS = [300, 900, 1800, 7200, 14400, 86400]

    def execute(self, command):
        try:
            response = requests.get(self.PUBLIC_API_URL + command).json()
        except Exception as e:
            raise PublicAPIError(e, command) from e

        if 'error' in response:
            raise PublicAPIError(response["error"], command, body=response)
        return response

    def get_orderbook(self, currencyPair, depth):
        """
        Returns the order book for a given market
        :param str currencyPair: A string that defines the market, "USDT_BTC" for example. Use "all" for all markets.
        :param int depth: Default depth is 50. Max depth is 100.
        :return: orderbook
        """
        command = "returnOrderBook&currencyPair={0}&depth={1}".format(currencyPair, depth)
        response = self.execute(command)
        return response

    def get_chartdata(self, currencyPair, period, start, end):
        """
        Returns candlestick chart data.
        :param str currencyPair: A string that defines the market, "USDT_BTC" for example.
        :param int period: Candlestick period in seconds. Valid values are 300, 900, 1800, 7200, 14400, and 86400.
        :param int start: The start of the window in seconds since the unix epoch.
        :param int end: The end of the window in seconds since the unix epoch.
        """
        if period not in self.PERIODS:
            raise PublicAPIError("bad_period", "returnChartData", body=period)

        if isinstance(start, str):
            start = self.date_to_unix_ts_in_utc(start)

        if isinstance(end, str):
            end = self.date_to_unix_ts_in_utc(end)

        command = "returnChartData&currencyPair={0}&start={1}&end={2}&period={3}".format(currencyPair, start, end,
                                                                                         period)
        response = self.execute(command)
        return response

    def get_trade_history(self, currencyPair, start, end):
        """
        Returns the past 200 trades for a given market,
        or up to 1,000 trades between a range specified in UNIX timestamps
        by the "start" and "end" GET parameters
        :param str currencyPair: A string that defines the market, "USDT_BTC" for example. Use "all" for all markets.
        :param int start: The start of the window in seconds since the unix epoch.
        :param int end: The end of the window in seconds since the unix epoch.
        :return: list of trade operation
        """
        if isinstance(start, str):
            start = self.date_to_unix_ts_in_utc(start)

        if isinstance(end, str):
            end = self.date_to_unix_ts_in_utc(end)

        command = "returnTradeHistory&currencyPair={0}&start={1}&end={2}"

        pages = []
        done = False
        while not done:
            response = self.execute(command.format(currencyPair, start, end))
            response.sort(key=lambda x: x["date"], reverse=True)

            if len(response):
                pages += response
                end = self.date_to_unix_ts_in_utc(response[-1]["date"]) - 1
                if end <= start:
                    done = True
            else:
                done = True
        return pages

    def get_trade_history_batch(self, currency_pair, start, end):
        """
        Returns the past 200 trades for a given market,
        or up to 1,000 trades between a range specified in UNIX timestamps
        by the "start" and "end" GET parameters
        :param str currency_pair: A string that defines the market, "USDT_BTC" for example. Use "all" for all markets.
        :param int start: The start of the window in seconds since the unix epoch.
        :param int end: The end of the window in seconds since the unix epoch.
        :return: list of trade operation
        """
        if isinstance(start, str):
            start = self.date_to_unix_ts_in_utc(start)

        if isinstance(end, str):
            end = self.date_to_unix_ts_in_utc(end)


        command = "returnTradeHistory&currencyPair={0}&start={1}&end={2}"
        done = False
        while not done:
            logger.debug("===================================================")
            logger.debug("Download data from {0} to {1}".format(start, end))
            response = self.execute(command.format(currency_pair, start, end))
            response.sort(key=lambda x: x["date"], reverse=True)
            logger.debug("len response: {}".format(len(response)))

            if len(response):
                logger.debug("first record: {}".format(response[0]))
                logger.debug("last record: {}".format(response[-1]))

                yield response
                end = self.date_to_unix_ts_in_utc(response[-1]["date"]) - 1
                logger.debug("new end: {}".format(end))
                if end <= start:
                    done = True
            else:
                logger.debug("response zero lenght")
                done = True

            logger.debug("done: {}".format(done))

    def get_tickers(self):
        """
        Retrieves summary information for each currency pair listed on the exchange
        :return: dict with tickers
        """
        command = "returnTicker"
        response = self.execute(command)
        return response

    def get_24hvolume(self):
        """
        Returns the 24-hour volume for all markets as well as totals for primary currencies.
        :return: dict with tickers volume
        """
        command = "return24hVolume"
        response = self.execute(command)
        return response

    def get_currencies(self):
        """
        Returns the 24-hour volume for all markets as well as totals for primary currencies.
        :return: dict with tickers volume
        """
        command = "returnCurrencies"
        response = self.execute(command)
        return response

    @staticmethod
    def date_to_unix_ts_in_utc(date):
        timezone = pytz.timezone("UTC")
        without_timezone = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        with_timezone = timezone.localize(without_timezone)
        return int(with_timezone.timestamp())
