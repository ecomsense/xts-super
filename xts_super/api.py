from stock_brokers.xts.xts import Xts
from constants import O_CNFG, CMMN
from typing import List
from traceback import print_exc


def login(api_name):
    try:
        cred = O_CNFG[api_name]
        if api_name == "xts_interactive":
            api = Xts(**cred)
            api.broker.interactive_login()
            return api
        elif api_name == "xts_marketdata":
            mapi = Xts(**cred)
            mapi.broker.marketdata_login()
            return mapi
    except Exception as e:
        print(e)
        print_exc()
        __import__("sys").exit(1)


class Helper:
    api = None
    mapi = None
    buy = []
    short = []
    paper = []

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
                if CMMN["live"]:
                    cls.api.order_place(**i)
                else:
                    cls.paper.append(i)

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
            if CMMN["live"]:
                cls.api.order_place(**o)
            else:
                cls.paper.append(o)
            lst.append(o)
