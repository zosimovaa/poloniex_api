import requests
import datetime
import pytz

# import logging
# logger = logging.getLogger(__name__)


class PublicAPIError(Exception):
    """Ошибка при обращении к публичному API"""
    CODES = {
        "req_error": "PUBLIC_API_ERROR:REQUEST_ERROR",
        "unknown_cmd": "PUBLIC_API_ERROR:UNKNOWN_COMMAND",
        "bad_period": "PUBLIC_API_ERROR:BAD_PERIOD"
    }

    def __init__(self, err_code, method, body=None):
        self.err_code = self.CODES[err_code]
        self.method = method
        self.body = body

    def __str__(self):
        message = "Code: {0}. Method: {1}".format(self.err_code, self.method)
        if self.body is not None:
            message += ". Response: {0})".format(self.body)
        return message


class PublicAPI:
    """Класс реализует работу с публичным API poloniex.com"""
    PUBLIC_API_URL = "https://poloniex.com/public?command="
    PERIODS = [300, 900, 1800, 7200, 14400, 86400]

    def execute(self, command):
        try:
            response_raw = requests.get(self.PUBLIC_API_URL + command)
            response = response_raw.json()
        except Exception as e:
            raise PublicAPIError("req_error", command, body=e)
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

                #clear_output(wait=True)
                #print("Осталось: {0}".format(end - start))

            else:
                done = True
                #clear_output(wait=True)
                #print("done")
        return pages

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
        return with_timezone.timestamp()