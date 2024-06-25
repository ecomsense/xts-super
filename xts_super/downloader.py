import requests
import pandas as pd
from constants import S_DATA, O_SETG
from toolkit.kokoo import timer


def get_symbols():
    ignore_keys_in_settings = ["supertrend", "common", "log"]
    keys_in_settings = list(O_SETG.keys())
    symbol_keys = [
        key for key in keys_in_settings if key not in ignore_keys_in_settings
    ]
    symbols = {key: O_SETG[key]["nse"] for key in symbol_keys}
    return symbols


def get_session(BASE) -> requests.Session:
    s = requests.Session()
    s.verify = True
    headers = {
        "Host": "www.nseindia.com",
        "Referer": "https://www.nseindia.com/get-quotes/equity?symbol=SBIN",
        "X-Requested-With": "XMLHttpRequest",
        "pragma": "no-cache",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }
    s.headers.update(headers)
    # s.headers.update({"User-Agent": _user_agent})
    s.get(BASE, timeout=(3.05, 10))
    return s


def pretify():
    print("*" * 100)


def main():
    NSE = "https://www.nseindia.com"
    API = NSE + "/api"
    symbols = get_symbols()
    for k, v in symbols.items():
        session = get_session(NSE)
        pretify()
        filepath = f"{S_DATA}{k}/history.csv"
        print(f"preparing ...download to {filepath}")
        dat = session.get(
            f"{API}/chart-databyindex",
            params={"index": v},
            timeout=(3.05, 10),
        ).json()
        df = pd.DataFrame(dat["grapthData"], columns=["timestamp", "close"])
        print(f"downloaded ... {len(df)} lines of data for {k}")

        df["timestamp_column"] = pd.to_datetime(df["timestamp"], unit="ms")
        start_time = pd.Timestamp("09:15:00")
        # Calculate the difference in minutes from the start time and round to the nearest 15-minute interval
        minute_diff = (df["timestamp_column"] - start_time).dt.total_seconds() / 60
        rounded_minute_diff = 15 * (minute_diff // 15)
        df["rounded_timestamp"] = start_time + pd.to_timedelta(
            rounded_minute_diff, unit="m"
        )
        print(df.head)
        # Filter the DataFrame to select the first occurrence of each rounded interval
        filtered_df = (
            df[df["rounded_timestamp"] >= start_time]
            .groupby(df["rounded_timestamp"].dt.floor("5min"))
            .first()
        )
        print(filtered_df.head())
        print(filtered_df.tail())
        # Reset index to make 'rounded_timestamp' a regular column
        filtered_df = filtered_df.reset_index(drop=True)
        # drop unwanted columns
        df.drop(columns=["timestamp_column", "rounded_timestamp"], inplace=True)
        print(f"reduced ... {len(filtered_df)} into lines of data for {k}")
        filtered_df.to_csv(filepath, header=True, index=False)
        timer(2)
        pretify()
        session.close()


if __name__ == "__main__":
    main()
