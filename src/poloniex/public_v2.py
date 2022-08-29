import requests
import datetime
import logging
import pytz

import pandas as pd

from basic_application import with_exception


logger = logging.getLogger(__name__)


class PublicApiError(Exception):
    def __init__(self, command=None, code=None, message="No message"):
        self.code = code
        self.command = command
        self.message = message
        super().__init__(self.message)


class PublicApiV2:
    HOST = "https://api.poloniex.com"
    INTERVALS = {
        "MINUTE_1": 60,
        "MINUTE_5": 300,
        "MINUTE_10": 600,
        "MINUTE_15": 900,
        "MINUTE_30": 1800,
        "HOUR_1": 3600,
        "HOUR_2": 7200,
        "HOUR_4": 14400,
        "HOUR_6": 21600,
        "HOUR_12": 43200,
        "DAY_1": 86400,
        "DAY_3": 3 * 86400,
        "WEEK_1": 7 * 86400,
        "MONTH_1": 30 * 86400
    }

    @with_exception(PublicApiError)
    def get_interval(self, key):
        return self.INTERVALS[key]

    def _execute(self, command):
        url = self.HOST + command
        logger.debug("Execute command:  {}".format(url))
        try:
            response = requests.get(url).json()
        except Exception as e:
            raise PublicApiError(command=command, message=str(e)) from e
        else:
            if "code" in response:
                logger.error(response)
                raise PublicApiError(command=command, **response)
            else:
                return response

    @with_exception(PublicApiError)
    def get_price(self, symbol):
        path = "/markets/{0}/price".format(symbol)
        response = self._execute(path)
        logger.debug("get_price return: {}".format(response))
        return response

    @with_exception(PublicApiError)
    def get_orderbook(self, symbol, scale=-1, limit=10):
        """
        Get the order book for a given symbol. Scale and limit values are optional.
        endpoint: /markets/{symbol}/orderBook
        @params
        - symbol: String [1] - symbol name
        - scale: String	[0..1] - controls aggregation by price
        - limit: Integer	[0..1] - maximum number of records returned.
        The default value of limit is 10. Valid limit values are: 5, 10, 20, 50, 100, 150.

        @return
        """
        path = "/markets/{0}/orderBook?scale={1}&limit={2}".format(symbol, scale, limit)
        response = self._execute(path)

        asks_keys = response["asks"][::2]
        asks_vals = map(float, response["asks"][1::2])
        asks = dict(zip(asks_keys, asks_vals))
        logger.debug("Orderbook asks len: {}".format(len(asks)))

        bids_keys = response["bids"][::2]
        bids_vals = map(float, response["bids"][1::2])
        bids = dict(zip(bids_keys, bids_vals))
        logger.debug("Orderbook bids len: {}".format(len(bids)))

        return asks, bids

    @with_exception(PublicApiError)
    def get_trades(self, symbol, limit=500):
        """
        endpoint: /markets/{symbol}/trades
        @params
        - symbol: String [1] - symbol name
        - limit: Integer [0..1] - maximum number of records returned. Default value is 500, and max value is 1000.
        @return
        """
        path = "/markets/{0}/trades?limit={1}".format(symbol, limit)
        response = self._execute(path)
        return response

    @with_exception(PublicApiError)
    def get_candles(self, symbol, interval, limit=100, start_time=None, end_time=None):
        """
        endpoint: /markets/{symbol}/candles
        @params
        - symbol: String [1] - symbol name
        - interval: String [1] - the unit of time to aggregate data by.
        Valid values: MINUTE_1, MINUTE_5, MINUTE_10, MINUTE_15, MINUTE_30, HOUR_1, HOUR_2, HOUR_4, HOUR_6, HOUR_12,
        DAY_1, DAY_3, WEEK_1 and MONTH_1
        - limit: Integer [0..1] - maximum number of records returned. The default value is 100 and the max value is 500.
        - startTime: Long [0..1] - filters by time. The default value is 0.
        - endTime: Long [0..1] - filters by time. The default value is current time

        """
        path = "/markets/{0}/candles?interval={1}&limit={2}".format(symbol, interval, limit)
        if start_time is not None:
            path = path + "&startTime={0}".format(int(start_time))

        if end_time is not None:
            path = path + "&endTime={0}".format(int(end_time))

        data = self._execute(path)
        columns = ["low", "high", "open", "close", "amount", "quantity", "buyTakerAmount",
                   "buyTakerQuantity", "tradeCount", "ts", "weightedAverage", "interval", "startTime", "closeTime"]

        df = pd.DataFrame(data, columns=columns, dtype=float)
        df["sellTakerAmount"] = df["amount"] - df["buyTakerAmount"]
        df["sellTakerQuantity"] = df["quantity"] - df["buyTakerQuantity"]

        df["ts"] = df["ts"].astype(int)
        df["startTime"] = df["startTime"].astype(int)
        df["closeTime"] = df["closeTime"].astype(int)
        df["tradeCount"] = df["tradeCount"].astype(int)

        df["symbol"] = symbol

        columns = ['symbol', 'low', 'high', 'open', 'close', 'amount', 'quantity', 'buyTakerAmount',
                   'buyTakerQuantity', 'sellTakerAmount', 'sellTakerQuantity', 'tradeCount', 'weightedAverage',
                   'interval',
                   'startTime', 'closeTime']
        logger.debug("Candles shape: {0} x {1}".format(*df.shape))
        logger.debug("Min ts: {0} | Max ts: {1}".format(
            self.unix_ts_to_date(df["startTime"].min()/1000.0),
            self.unix_ts_to_date(df["startTime"].max()/1000.0)))

        return df[columns]

    @staticmethod
    def date_to_unix_ts_in_utc(date):
        if isinstance(date, str):
            timezone = pytz.timezone("UTC")
            without_timezone = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
            with_timezone = timezone.localize(without_timezone)
            return int(with_timezone.timestamp() * 1000)
        else:
            return int(date)

    @staticmethod
    def unix_ts_to_date(unix_ts):
        return datetime.datetime.utcfromtimestamp(int(unix_ts)).strftime('%Y-%m-%d %H:%M:%S.%f')
