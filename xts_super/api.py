from stock_brokers.xts.xts import Xts
from constants import O_CNFG


def login(api_name):
    cred = O_CNFG[api_name]
    api = Xts(**cred)
    if api_name == "xts_interactive":
        api.broker.interactive_login()
    elif api_name == "xts_marketdata":
        api.broker.marketdata_login()
    return api


class Helper:
    api = None
    mapi = None

    @classmethod
    def set_api(cls):
        if cls.api is None:
            cls.api = login("xts_interactive")

    @classmethod
    def set_mapi(cls):
        if cls.mapi is None:
            cls.mapi = login("xts_marketdata")
