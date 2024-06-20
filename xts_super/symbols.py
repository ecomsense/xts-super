from constants import S_DATA, O_FUTL
import re
from typing import Dict, Union
import pandas as pd

exch = {
    "NSE": {"id": 1, "code": "NSECM"},
    "NFO": {"id": 2, "code": "NSEFO"},
}
msg_code = {
    "instrument_change": 1105,
    "touchline": 1501,
    "market_data": 1502,
    "candle_data": 1505,
    "market_status": 1507,
    "oi": 1510,
    "ltp": 1512,
}
dct_sym = {
    "NIFTY": {
        "diff": 50,
        "index": "NIFTY 50",
        "exch": "NSE",
        "token": "26000",
        "depth": 16,
    },
    "BANKNIFTY": {
        "diff": 100,
        "index": "NIFTY BANK",
        "exch": "NSE",
        "token": "26001",
        "depth": 25,
    },
}


class Symbols:
    def __init__(self, exchange: str, symbol: str, expiry: str):
        self.exchange = exchange
        self.symbol = symbol
        self.expiry = expiry
        self.dumpfile = f"{S_DATA}{self.exchange}_symbols.txt"

    def dump(self, data: Union[str, str]) -> None:
        if O_FUTL.is_file_not_2day(self.dumpfile):
            header = "ExchangeSegment | ExchangeInstrumentID | InstrumentType | Name | Description | Series | NameWithSeries | InstrumentID | PriceBand.High | PriceBand.Low | FreezeQty | TickSize | LotSize | Multiplier | UnderlyingInstrumentId | UnderlyingIndexName | ContractExpiration | StrikePrice | OptionType | displayName | PriceNumerator | PriceDenominator"
            header += "\n"
            with open(self.dumpfile, "w") as file:
                file.write(header)
                file.write(data)

    def find_symbol_from_token(self, inst_id: int) -> str:
        def is_int(string):
            try:
                int(string)
                return True
            except ValueError:
                return False

        with open(self.dumpfile, "r") as file:
            contents = file.read()
            records = contents.split("\n")
            print(f"searching in {len(records)} records for {inst_id}")
            for record in records:
                fields = record.split("|")
                if is_int(fields[1]) and int(fields[1]) == inst_id:
                    inst = fields[4]
                    return inst
        return "INSTRUMENT_NOT_FOUND"

    def find_token_from_symbol(self, inst: str) -> int:
        with open(self.dumpfile, "r") as file:
            contents = file.read()
            records = contents.split("\n")
            for record in records:
                fields = record.split("|")
                if fields[4] == inst:
                    int_inst = fields[1]
                    return int_inst
        return 0

    def find_tokens_from_symbols(self, lst_inst: list) -> list:
        """
        consumed by get quotes method
        """
        lst_dct_inst = []
        for inst in lst_inst:
            dct_inst = {}
            dct_inst["exchangeSegment"] = exch[self.exchange]["id"]
            dct_inst["exchangeInstrumentID"] = self.find_token_from_symbol(inst)
            lst_dct_inst.append(dct_inst)
        return lst_dct_inst

    def find_option_by_distance(
        self, atm: int, distance: int, c_or_p: str, dct_symbols: dict
    ):
        match = {}
        if c_or_p == "C":
            find_strike = atm + (distance * dct_sym[self.symbol]["diff"])
        else:
            find_strike = atm - (distance * dct_sym[self.symbol]["diff"])
        option_pattern = self.symbol + self.expiry + c_or_p + str(find_strike)
        for k, v in dct_symbols.items():
            if v == option_pattern:
                match.update({"symbol": v, "token": k.split("|")[-1]})
                break
        if any(match):
            return match
        else:
            raise Exception("Option not found")

    """
    not used
    """

    def get_atm(self, ltp) -> int:
        current_strike = ltp - (ltp % dct_sym[self.symbol]["diff"])
        next_higher_strike = current_strike + dct_sym[self.symbol]["diff"]
        if ltp - current_strike < next_higher_strike - ltp:
            return int(current_strike)
        return int(next_higher_strike)

    def parse_option_type(self, tradingsymbol):
        option_pattern = re.compile(rf"{self.symbol}{self.expiry}([CP])\d+")
        match = option_pattern.match(tradingsymbol)
        if match:
            return match.group(1)  # Returns 'C' for call, 'P' for put
        else:
            return False

    def build_option_chain(self, strike):
        df = pd.read_csv(self.dumpfile)
        lst = []
        lst.append(self.symbol + self.expiry + "C" + str(strike))
        lst.append(self.symbol + self.expiry + "P" + str(strike))
        for v in range(1, dct_sym[self.symbol]["depth"]):
            lst.append(
                self.symbol
                + self.expiry
                + "C"
                + str(strike + v * dct_sym[self.symbol]["diff"])
            )
            lst.append(
                self.symbol
                + self.expiry
                + "P"
                + str(strike + v * dct_sym[self.symbol]["diff"])
            )
            lst.append(
                self.symbol
                + self.expiry
                + "C"
                + str(strike - v * dct_sym[self.symbol]["diff"])
            )
            lst.append(
                self.symbol
                + self.expiry
                + "P"
                + str(strike - v * dct_sym[self.symbol]["diff"])
            )

        df["Exchange"] = self.exchange
        tokens_found = (
            df[df["TradingSymbol"].isin(lst)]
            .assign(tknexc=df["Exchange"] + "|" + df["Token"].astype(str))[
                ["tknexc", "TradingSymbol"]
            ]
            .set_index("tknexc")
        )
        dct = tokens_found.to_dict()
        return dct["TradingSymbol"]


if __name__ == "__main__":
    sym = Symbols("NSE", "", "")
    resp = sym.find_token_from_symbol("BANKNIFTY-EQ")
    print(resp)
