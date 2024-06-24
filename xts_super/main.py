from constants import logging, CMMN, SYMBOL, S_DATA, SUPR, BASE
from toolkit.kokoo import is_time_past, blink, timer
from api import Helper
from symbols import Symbols, exch, msg_code, dct_sym
from renkodf import RenkoWS
import streaming_indicators as si
from datetime import datetime as dt
import pandas as pd
import numpy as np
from traceback import print_exc
import json
from typing import Dict, List

F_HIST = f"{S_DATA}{SYMBOL}/history.csv"

# common
START = CMMN["start"]
EOD = CMMN["eod"]
LIVE = CMMN["live"]
EXCHANGE = CMMN["exchange"]

# symbol settings
B_EXPIRY = BASE["bexpiry"]
S_EXPIRY = BASE["sexpiry"]
B_DIFF = BASE["bdiff"]
S_DIFF = BASE["sdiff"]

# local constants
G_MODE_TRADE = False
MAGIC = 15


def get_ltp():
    blink()
    args = [
        {
            "exchangeSegment": exch["NSE"]["id"],
            "exchangeInstrumentID": dct_sym[SYMBOL]["token"],
        }
    ]
    resp = Helper.api.broker.get_quote(args, msg_code["ltp"], "JSON")
    ltp = json.loads(resp["result"]["listQuotes"][0])["LastTradedPrice"]
    print(f"{ltp=}")
    ltp = 55000
    return ltp


Helper.set_api()
Helper.set_mapi()

# symbol objects
B_SYM = Symbols(EXCHANGE, SYMBOL, B_EXPIRY)
resp = Helper.api.broker.get_master([exch[EXCHANGE]["code"]])
if resp:
    B_SYM.dump(resp)

Helper.ltp = get_ltp()
atm = B_SYM.calc_atm_from_ltp(Helper.ltp)
b_tknsym: Dict = B_SYM.build_option_chain(atm)

S_SYM = Symbols(EXCHANGE, SYMBOL, S_EXPIRY)
s_tknsym: Dict = S_SYM.build_option_chain(atm)


def find_symbol(buy_or_short: str, order_args: Dict):
    diff = B_DIFF if order_args["side"] == "B" else S_DIFF
    if buy_or_short == "buy":
        atm = B_SYM.calc_atm_from_ltp(Helper.ltp)
        option = B_SYM.find_option_by_distance(
            atm,
            diff,
            "CE",
        )
    else:
        atm = S_SYM.calc_atm_from_ltp(Helper.ltp)
        option = S_SYM.find_option_by_distance(
            atm,
            diff,
            "PE",
        )
        segment = exch[S_SYM.exchange]["id"]
        key = S_SYM.exchange + "|" + option
    order_args["symbol"] = segment + ":" + s_tknsym[key]
    print(order_args)
    return order_args


def split_colors(st: pd.DataFrame):
    global G_MODE_TRADE
    try:
        new_pos = {}
        UP = []
        DN = []
        for i in range(len(st)):
            if st["st_dir"].iloc[i] == 1:
                UP.append(st["st"].iloc[i])
                DN.append(np.nan)
            elif st["st_dir"].iloc[i] == -1:
                DN.append(st["st"].iloc[i])
                UP.append(np.nan)
            else:
                UP.append(np.nan)
                DN.append(np.nan)
        st["up"] = UP
        st["dn"] = DN

        if len(st) > 1:
            dets = st.iloc[-2:-1].copy()
            dets["timestamp"] = dt.now()
            dets.drop(columns=["high", "low", "up", "dn", "st_dir"], inplace=True)

            # we are not live yet
            if not G_MODE_TRADE:
                if st.iloc[-1]["volume"] > MAGIC:
                    G_MODE_TRADE = True
            else:
                if (
                    dets.iloc[-1]["close"] > dets.iloc[-1]["st"]
                    and not any(Helper.buy)
                    and dets.iloc[-1]["close"] > dets.iloc[-1]["open"]
                ):
                    Helper.exit("short")
                    # list of args for entry
                    lst = []
                    bargs = dict(
                        side="B",
                        quantity=BASE["quantity"],
                    )
                    bargs = find_symbol("buy", bargs)
                    lst.append(bargs)
                    sargs = dict(
                        side="S",
                        quantity=BASE["quantity"],
                    )
                    sargs = find_symbol("buy", sargs)
                    lst.append(sargs)
                    Helper.enter("buy", lst)
                elif (
                    dets.iloc[-1]["close"] < dets.iloc[-1]["st"]
                    and not any(Helper.short)
                    and dets.iloc[-1]["close"] < dets.iloc[-1]["open"]
                ):
                    Helper.exit("buy")
                    # list of args for entry
                    lst = []
                    args = dict(symbol=SYMBOL)
                    lst.append(args)
                    Helper.enter("short", lst)

                print("Signals \n", dets)
            print("Data \n", st.tail(2))
        print(f"Ready to take Trade ? {G_MODE_TRADE}")
    except KeyboardInterrupt:
        print("Exiting ...")
    except Exception as e:
        logging.warning(f"{e} while splitting colors")
        print_exc()
    return st, new_pos


def main():
    try:
        df_ticks = pd.read_csv(F_HIST)
        r = RenkoWS(
            df_ticks["timestamp"].iat[0],
            df_ticks["close"].iat[0],
            brick_size=BASE["brick"],
        )
        r.initial_df

        # init super trend streaming indicator
        ST = si.SuperTrend(SUPR["atr"], SUPR["multiplier"])
    except Exception as e:
        print("main:", e)

    def run(ival=None):
        try:
            df_normal = pd.DataFrame()
            if not ival:
                ival = len(df_ticks)

            Helper.ltp = get_ltp()
            if Helper.ltp == 0:
                return df_normal

            df_ticks.loc[len(df_ticks)] = {
                "timestamp": dt.now().timestamp(),
                "Symbol": SYMBOL,
                "close": Helper.ltp,
            }
            r.add_prices(
                df_ticks["timestamp"].iat[(0 + ival)], df_ticks["close"].iat[(0 + ival)]
            )
            df_normal = r.renko_animate("normal", max_len=MAGIC, keep=MAGIC - 1)
            for key, candle in df_normal.iterrows():
                st_dir, st = ST.update(candle)
                # add the st value to respective row
                # in the dataframe
                df_normal.loc[key, "st"] = st
                df_normal.loc[key, "st_dir"] = st_dir

            # get direction and split colors of supertrend
            df_normal, new_pos = split_colors(df_normal)
            # df_normal.to_csv(DATA + "df_normal.csv")
            # update positions if they are available
            if any(new_pos):
                logging.debug(f"found {new_pos=}")
                # D_POS.update(new_pos)
            timer(5)

        except KeyboardInterrupt:
            __import__("sys").exit(0)

        except Exception as e:
            logging.error(f"{e} while common func")
            print(e)
        finally:
            return df_normal

    while is_time_past(CMMN["start"]):
        timer(2)
        print("starting ..")
        _ = run()
    else:
        run()


main()
