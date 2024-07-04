from constants import logging, O_CNFG, CMMN, S_DATA
from stock_brokers.xts.xts import Xts
from typing import List
from traceback import print_exc
import json
from toolkit.kokoo import timer
import pandas as pd
import pendulum as plum


def ord_to_pos(df):
    # Filter DataFrame to include only 'B' (Buy) side transactions
    buy_df = df[df["side"] == "B"]

    # Filter DataFrame to include only 'S' (Sell) side transactions
    sell_df = df[df["side"] == "S"]

    # Group by 'symbol' and sum 'filled_quantity' for 'B' side transactions
    buy_grouped = (
        buy_df.groupby("symbol")
        .agg({"filled_quantity": "sum", "average_price": "sum"})
        .reset_index()
    )
    # Group by 'symbol' and sum 'filled_quantity' for 'S' side transactions
    sell_grouped = (
        sell_df.groupby("symbol")
        .agg({"filled_quantity": "sum", "average_price": "sum"})
        .reset_index()
    )
    # Merge the two DataFrames on 'symbol' column with a left join
    result_df = pd.merge(
        buy_grouped,
        sell_grouped,
        on="symbol",
        suffixes=("_buy", "_sell"),
        how="outer",
    )

    result_df.fillna(0, inplace=True)
    # Calculate the net filled quantity by subtracting 'Sell' side quantity from 'Buy' side quantity

    result_df["quantity"] = (
        result_df["filled_quantity_buy"] - result_df["filled_quantity_sell"]
    )
    result_df["urmtom"] = result_df.apply(
        lambda row: 0
        if row["quantity"] == 0
        else (row["average_price_buy"] - row["filled_quantity_sell"]) * row["quantity"],
        axis=1,
    )
    result_df["rpnl"] = result_df.apply(
        lambda row: row["average_price_sell"] - row["average_price_buy"]
        if row["quantity"] == 0
        else 0,
        axis=1,
    )
    result_df.drop(
        columns=[
            "filled_quantity_buy",
            "filled_quantity_sell",
            "average_price_buy",
            "average_price_sell",
        ],
        inplace=True,
    )
    return result_df


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


class Helper:
    api = None
    mapi = None
    buy = []
    short = []
    orders = []

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
        try:
            lst = cls.buy if buy_or_short == "buy" else cls.short
            print(f"exiting {buy_or_short}")

            if any(lst):
                for i in lst:
                    side = i.pop("side")
                    i["side"] = "S" if side == "B" else "B"
                    i["tag"] = "exit"
                    if CMMN["live"]:
                        resp = cls.api.order_place(**i)
                        print(resp)
                    else:
                        cls.orders.append(i)

                if buy_or_short == "buy":
                    cls.buy = []
                else:
                    cls.short = []
        except Exception as e:
            logging.error(f"exit: {e}")
            print_exc()

    @classmethod
    def enter(cls, buy_or_short: str, orders: List):
        """
        param orders:
            contains dictionary with keys
            symbol, side, quantity, price, trigger_price
        """
        try:
            print(f"entering {buy_or_short}")
            lst = cls.buy if buy_or_short == "buy" else cls.short
            for o in orders:
                o["validity"] = "DAY"
                o["product"] = "NRML"
                logging.debug(o)
                if CMMN["live"]:
                    resp = cls.api.order_place(**o)
                    print(resp)
                else:
                    args = [
                        {
                            "exchangeSegment": 2,
                            "exchangeInstrumentID": o["symbol"].split("|")[-1],
                        }
                    ]
                    o["broker_timestamp"] = plum.now().format("YYYY-MM-DD HH:mm:ss")
                    o["average_price"] = Helper.get_ltp(args)
                    o["filled_quantity"] = o.pop("quantity")
                    o["tag"] = "enter"
                    cls.orders.append(o)
                lst.append(o)
        except Exception as e:
            logging.error(f"enter: {e}")
            print_exc()

    @classmethod
    def get_ltp(cls, args, xtsMessageCode=1512, publishFormat="JSON"):
        try:
            resp = cls.mapi.broker.get_quote(args, xtsMessageCode, publishFormat)
            if resp is not None:
                logging.debug(f"get_underlying {resp=}")
                str_resp = resp["result"]["listQuotes"][0]
                logging.debug(f"{str_resp=}")
                jsn_resp = json.loads(str_resp)
                logging.debug(f"{jsn_resp=}")
                ltp = jsn_resp["LastTradedPrice"]
                logging.info(f"{ltp=}")
                return ltp
            else:
                cls.mapi = None
                cls.set_mapi()
                cls.get_ltp(args, xtsMessageCode)
        except Exception as e:
            logging.error(f"ltp: {e}")
            print_exc()

    @classmethod
    def positions(cls):
        if CMMN["live"]:
            return cls.api.positions
        elif any(cls.orders):
            df = pd.DataFrame(cls.orders)
            df.to_csv(S_DATA + "orders.csv", index=False)
            df = ord_to_pos(df)
            lst = df.to_dict(orient="records")
            return lst
        else:
            return []


if __name__ == "__main__":
    Helper.set_mapi()
    Helper.set_api()

    helper = Helper()
    args = [
        {
            "exchangeSegment": 1,
            "exchangeInstrumentID": 26001,
        }
    ]
    logging.debug(f"{args=}")

    resp = Helper.api.positions
    pd.DataFrame(resp).to_csv(S_DATA + "positions.csv", index=False)

    resp = Helper.api.orders
    pd.DataFrame(resp).to_csv(S_DATA + "orders.csv", index=False)
    while True:
        timer(1)
        ltp = helper.get_ltp(args, 1512)
        print(ltp)
