from constants import O_CNFG
from stock_brokers.xts.xts import Xts


def get_token():
    cred = O_CNFG["xts_interactive"]
    api = Xts(**cred)
    if api.authenticate():
        return api


api = get_token()
print(api.token)
print(api.broker.get_profile())
