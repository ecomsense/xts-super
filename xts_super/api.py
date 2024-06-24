from stock_brokers.xts.xts import Xts
from constants import O_CNFG
from typing import List


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
    buy = []
    short = []

    @classmethod
    def set_api(cls):
        if cls.api is None:
            cls.api = login("xts_interactive")

    @classmethod
    def set_mapi(cls):
        if cls.mapi is None:
            cls.mapi = login("xts_marketdata")

    @classmethod
    def exit(cls, buy_or_short: str):
        lst = cls.buy if buy_or_short == "buy" else cls.short
        if any(lst):
            for i in lst:
                side = i.pop("side")
                i["side"] = "S" if side == "B" else "B"
                i["tag"] = "exit"
                cls.api.order_place(**i)
            lst = []

    @classmethod
    def enter(cls, buy_or_short: str, orders: List):
        """
        param orders:
            contains dictionary with keys
            symbol, side, quantity, price, trigger_price
        """
        lst = cls.buy if buy_or_short == "buy" else cls.short
        for o in orders:
            o["validity"] = "DAY"
            o["product"] = "MIS"
            cls.api.order_place(**o)
            lst.append(o)
