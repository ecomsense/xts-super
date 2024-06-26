from constants import logging, O_CNFG, CMMN
from stock_brokers.xts.xts import Xts
from typing import List
from traceback import print_exc
import json
from toolkit.kokoo import timer


def login(api_name):
    try:
        cred = O_CNFG[api_name]
        api = Xts(**cred)
        if api_name == "xts_interactive":
            api.broker.interactive_login()
        elif api_name == "xts_marketdata":
            api.broker.marketdata_login()
        logging.debug(f"logging in {api_name}")
        logging.debug(cred)
        """
        logging.info(api.broker.get_profile(clientID=cred["userID"]))
        """
        return api
    except Exception as e:
        logging.error(e)
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
            logging.info(o)
            o["validity"] = "DAY"
            o["product"] = "MIS"
            if CMMN["live"]:
                cls.api.order_place(**o)
            else:
                cls.paper.append(o)
            lst.append(o)

    @classmethod
    def get_ltp(cls, args, xtsMessageCode, publishFormat="JSON"):
        try:
            resp = cls.mapi.broker.get_quote(args, xtsMessageCode, publishFormat)
            if resp is not None:
                logging.debug(f"get_underlying {resp=}")
                str_resp = resp["result"]["listQuotes"][0]
                logging.debug(f"{str_resp=}")
                jsn_resp = json.loads(str_resp)
                logging.debug(f"{jsn_resp=}")
                ltp = jsn_resp["LastTradedPrice"]
                return ltp
            else:
                cls.mapi = None
                cls.set_mapi()
                cls.get_ltp(args, xtsMessageCode)
        except Exception as e:
            logging.error(f"ltp: {e}")
            print_exc()
