import pandas as pd
from ens import ENS
from ens.exceptions import InvalidName
from web3 import Web3, HTTPProvider
from web3.exceptions import BadFunctionCallOutput, ContractLogicError
from multiprocessing import Pool
from tqdm import tqdm
from math import ceil
from time import sleep
from requests.exceptions import HTTPError, ReadTimeout, ConnectionError
import warnings
import backoff
import sys

warnings.filterwarnings("ignore")

NUMBER_OF_THREADS = 50
STEP = 10_000
ETH_URL = 'https://rpc.ethereum.cybernode.ai'

ens_names = pd.read_csv('data/ens_names.csv')
ens_names_list = ens_names.name.to_list()

EXCEPTIONS = (HTTPError, ReadTimeout, BadFunctionCallOutput, ContractLogicError, ConnectionError, ValueError,
              ConnectionResetError,)
MAX_TRIES = 5
SLEEP_TIME = 5


def backoff_handler(details):
    print(f'Exception occurred: {sys.exc_info()[0]} {sys.exc_info()[1:]}')
    print(details)
    sleep(SLEEP_TIME)


def exception_handler(function):
    function = backoff.on_exception(
        backoff.expo,
        exception=EXCEPTIONS,
        on_backoff=backoff_handler,
        on_giveup=backoff_handler,
        max_tries=MAX_TRIES
    )(function)
    return function


@exception_handler
def get_ens_data(name: str) -> dict:
    """
    Extract ENS data (resolver, owner and associated addresses, contenthash) from ethereum node by name
    :param name: ENS name without `.eth`
    :return: dict with resolver, owner and associated addresses, contenthash
    """
    _w3 = Web3(HTTPProvider(ETH_URL))
    _ns = ENS.fromWeb3(_w3)
    _name = str(name) + '.eth'
    _owner = _resolver = _resolver_address = _associated_address = _contenthash = None
    try:
        _owner = _ns.owner(_name)
        _resolver = _ns.resolver(_name)
    except InvalidName as e:
        print(name, e)
    except ValueError as e:
        print(name, e)
    if _resolver:
        _resolver_address = _resolver.address
        try:
            _associated_address = _ns.address(_name)
            _contenthash = _ns.resolver(_name).functions.contenthash(_ns.namehash(_name)).call()
            _contenthash = _contenthash.hex()
        except (ContractLogicError, BadFunctionCallOutput):
            pass
        except ValueError:
            try:
                _contenthash = _ns.resolver(_name).functions.content(_ns.namehash(_name)).call()
                _contenthash = _contenthash.hex()
            except ValueError:
                pass
    return {
        "name": _name,
        "owner": _owner,
        "resolver_address": _resolver_address,
        "associated_address": _associated_address,
        "contenthash": _contenthash
    }


if __name__ == '__main__':
    print(f'Number of names: {len(ens_names_list)}')
    for i in tqdm(range(ceil(len(ens_names_list) / STEP))):
        tasks = ens_names_list[i * STEP:min((i + 1) * STEP, len(ens_names_list))]
        print(f'Number of tasks: {len(tasks)} from {i * STEP} to {min((i + 1) * STEP, len(ens_names_list))}')
        print(f'Number of threads: {NUMBER_OF_THREADS}')
        with Pool(processes=NUMBER_OF_THREADS) as pool:
            ens_data_list = list(tqdm(pool.imap(get_ens_data, tasks), total=len(tasks)))

        ens_data_df = pd.DataFrame([item for item in ens_data_list if item and item.get('name') is not None])
        ens_data_df.to_csv(f'data/temp/ens_data_{i}.csv')
        print(f'{i} length {len(ens_data_df)}')
