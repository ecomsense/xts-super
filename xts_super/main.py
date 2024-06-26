from constants import O_FUTL, logging, CMMN, SYMBOL, S_DATA, SUPR, BASE
from toolkit.kokoo import is_time_past, timer
from api import Helper
from symbols import Symbols, exch, msg_code, dct_sym
from renkodf import RenkoWS
import streaming_indicators as si
from datetime import datetime as dt
import pandas as pd
import numpy as np
from traceback import print_exc
from typing import Dict
from pprint import pprint

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


def get_underlying():
    try:
        ltp = 50000
        timer(5)
        args = [
            {
                "exchangeSegment": exch["NSE"]["id"],
                "exchangeInstrumentID": dct_sym[SYMBOL]["token"],
            }
        ]
        logging.debug(f"{args=}")
        ltp = Helper.get_ltp(args, msg_code["ltp"])
    except Exception as e:
        logging.error(f"get_underlying: {e}")
        print_exc()
    finally:
        return ltp


try:
    Helper.set_api()
    Helper.set_mapi()

    # symbol objects
    B_SYM = Symbols(EXCHANGE, SYMBOL, B_EXPIRY)
    resp = Helper.api.broker.get_master([exch[EXCHANGE]["code"]])
    if resp:
        B_SYM.dump(resp)

    Helper.ltp = get_underlying()
    atm = B_SYM.calc_atm_from_ltp(Helper.ltp)
    b_tknsym: Dict = B_SYM.build_option_chain(atm)

    S_SYM = Symbols(EXCHANGE, SYMBOL, S_EXPIRY)
    s_tknsym: Dict = S_SYM.build_option_chain(atm)
except Exception as e:
    logging.error("bootstrap: ", e)
    print_exc()


def find_symbol(buy_or_short: str, order_args: Dict):
    try:
        diff = B_DIFF if order_args["side"] == "B" else S_DIFF
        if buy_or_short == "buy":
            atm = B_SYM.calc_atm_from_ltp(Helper.ltp)
            option = B_SYM.find_option_by_distance(atm, diff, "CE", b_tknsym)
            segment = exch[B_SYM.exchange]["id"]
            key = S_SYM.exchange + "|" + option["token"]
        else:
            atm = S_SYM.calc_atm_from_ltp(Helper.ltp)
            option = S_SYM.find_option_by_distance(atm, diff, "PE", s_tknsym)
            segment = exch[S_SYM.exchange]["id"]
            key = S_SYM.exchange + "|" + option["token"]
        order_args["symbol"] = str(segment) + ":" + s_tknsym[key]
        logging.debug(f"{order_args=}")
        return order_args
    except Exception as e:
        logging.error("find_symbol: ", e)
        print_exc()


def split_colors(st: pd.DataFrame):
    global G_MODE_TRADE
    try:
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
    return st


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
        logging.error("main:", e)
        print_exc()

    def run(ival=None):
        try:
            if not ival:
                ival = len(df_ticks)

            Helper.ltp = get_underlying()
            if Helper.ltp == 0:
                return pd.DataFrame()

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
            df_normal = split_colors(df_normal)
            """
            # df_normal.to_csv(DATA + "df_normal.csv")
            # update positions if they are available
            if any(new_pos):
                logging.debug(f"found {new_pos=}")
                D_POS.update(new_pos)
            """
            timer(1)
            pprint(Helper.paper)
            O_FUTL.write_file(S_DATA + "papper.json", Helper.paper)
            return df_normal
        except KeyboardInterrupt:
            __import__("sys").exit(0)

        except Exception as e:
            logging.error(f"{e} while common func")
            print_exc()

    while is_time_past(CMMN["start"]):
        timer(1)
        _ = run()
    else:
        # TODO remove this after testing
        run()


main()
