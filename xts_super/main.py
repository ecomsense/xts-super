from constants import logging, CMMN, DIST, EXPIRY, SYMBOL, S_DATA
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

# Data directory is mutated in constants file
F_HIST = S_DATA + "history.csv"

# common
START = CMMN["start"]
EOD = CMMN["eod"]
LIVE = CMMN["live"]
EXCHANGE = CMMN["exchange"]

# expiries
B_EXPIRY = EXPIRY["buy"]
S_EXPIRY = EXPIRY["sell"]

# distance of options from ATM
B_DIST = DIST["buy"]
S_DIST = DIST["sell"]


# local constants
G_MODE_TRADE = False
MAGIC = 15


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
                    if st.iloc[-1]["st_dir"] == 1 and call_or_put_pos() != "C":
                        new_pos = do(dets, "C")
                    elif st.iloc[-1]["st_dir"] == -1 and call_or_put_pos() != "P":
                        new_pos = do(dets, "P")
            else:
                if (
                    dets.iloc[-1]["close"] > dets.iloc[-1]["st"]
                    and call_or_put_pos() != "C"
                    and dets.iloc[-1]["close"] > dets.iloc[-1]["open"]
                ):
                    new_pos = do(dets, "C")
                elif (
                    dets.iloc[-1]["close"] < dets.iloc[-1]["st"]
                    and call_or_put_pos() != "P"
                    and dets.iloc[-1]["close"] < dets.iloc[-1]["open"]
                ):
                    new_pos = do(dets, "P")
                print("Signals \n", dets)
            print("Data \n", st.tail(2))
        print(f"Ready to take Trade ? {G_MODE_TRADE}")
    except Exception as e:
        logging.warning(f"{e} while splitting colors")
        print_exc()
    return st, new_pos


def main():
    Helper.set_api()
    Helper.set_mapi()

    # exchangesegments = [iApi.broker.EXCHANGE_NSEFO]
    # symbol objects
    B_SYM = Symbols(EXCHANGE, SYMBOL, B_EXPIRY)
    resp = Helper.api.broker.get_master([exch[EXCHANGE]["code"]])["result"]
    B_SYM.dump(resp)

    # get ltp
    args = [
        {
            "exchangeSegment": exch["NSE"]["id"],
            "exchangeInstrumentID": dct_sym[SYMBOL]["token"],
        }
    ]
    resp = Helper.api.broker.get_quote(args, msg_code["ltp"], "JSON")
    ltp = json.loads(resp["result"]["listQuotes"][0])["LastTradedPrice"]
    print(ltp)

    atm = B_SYM.get_atm(ltp)
    B_SYM.build_option_chain(atm)

    S_SYM = Symbols(EXCHANGE, SYMBOL, S_EXPIRY)
    """
    dct_symtkns = B_SYM.build_option_chain()
    df_ticks = pd.read_csv(F_HIST)
    r = RenkoWS(
        df_ticks["timestamp"].iat[0],
        df_ticks["close"].iat[0],
        brick_size=SETG[SYMBOL]["brick"],
    )
    r.initial_df
    # init super trend streaming indicator
    ST = si.SuperTrend(SUPR["atr"], SUPR["multiplier"])

    def run(ival=None):
        df_normal = pd.DataFrame()
        try:
            if not ival:
                ival = len(df_ticks)

            ulying = get_ltp(O_API)
            if ulying == 0:
                return df_normal

            df_ticks.loc[len(df_ticks)] = {
                "timestamp": dt.now().timestamp(),
                "Symbol": SYMBOL,
                "close": ulying,
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
                D_POS.update(new_pos)

        except Exception as e:
            logging.error(f"{e} while common func")
            print(e)
        finally:
            return df_normal

    while is_time_past("09:15"):
        _ = run(0)
    """


main()
