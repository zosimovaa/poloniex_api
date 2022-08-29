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


    @staticmethod
    def date_to_unix_ts_in_utc(date):
        timezone = pytz.timezone("UTC")
        without_timezone = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        with_timezone = timezone.localize(without_timezone)
        return int(with_timezone.timestamp())
