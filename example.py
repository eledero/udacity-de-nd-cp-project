import urllib
import time

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import bitstamp.client
from etherscan import Etherscan


eth_bitstamp_urls = [
    "https://www.cryptodatadownload.com/cdd/Bitstamp_ETHUSD_2021_minute.csv",
    "https://www.cryptodatadownload.com/cdd/Bitstamp_ETHUSD_2020_minute.csv",
    "https://www.cryptodatadownload.com/cdd/Bitstamp_ETHUSD_2019_minute.csv"
]

# Etherscan
YOUR_API_KEY = 'D61F9X3T2ECDGI6YHPV45UFGR6TE9M72V8'
eth = Etherscan(YOUR_API_KEY)
print(eth.get_eth_nodes_size(
                            start_date = '2021-11-01',
                            end_date='2021-11-29',
                            client_type='geth',
                            sync_mode='default',
                            sort='asc'))

# Bitstamp
public_client = bitstamp.client.Public()
print(public_client.ticker()['volume'])

"""
# Download Ethereum Price and Volume data

for url in eth_bitstamp_urls:

    file_name = url.split("Bitstamp_")[-1]
    urllib.request.urlretrieve(url, file_name)

"""

# Perform quality checks

for url in eth_bitstamp_urls:

    file_name = url.split("Bitstamp_")[-1]
    eth_data = pd.read_csv(
                            file_name,
                            skiprows=1
                            ).rename(
                                columns={
                                        "unix":"unix_timestamp",
                                        "date":"date_time",
                                        "symbol":"currency_pair",
                                        "open":"open_price",
                                        "high":"high_price",
                                        "low":"low_price",
                                        "close":"close_price",
                                        'Volume ETH':'volume_1',
                                        'Volume USD':'volume_2'})

    # Check data types, as per data_types list
    columns = list(eth_data.columns)
    data_types = [np.int64, np.object, np.object, np.float64, np.float64,
                    np.float64, np.float64, np.float64, np.float64]
    assert all([eth_data[columns[i]].dtype == data_types[i]
                for i in range(len(columns))])

    # Unique value for the currency_pair column should be 'ETH/USD':
    assert eth_data['currency_pair'].unique() == ['ETH/USD']

    # Unix timestamps can't be greater than now's timestamp:
    assert int(time.time()) >= eth_data['unix_timestamp'].max()

    # Datetime cannot be more recent than now
    assert pd.to_datetime(eth_data['date_time']).max() <= \
        pd.to_datetime(int(time.time()), unit = 's')

    print(pd.to_datetime(eth_data['date_time']).max())
    print(pd.to_datetime(int(time.time()), unit = 's'))
    print("Data checks OK!")




"""
    {
    'unix_timestamp': dtype('int64'),
    'date_time': dtype('O'),
    'currency_pair': dtype('O'),
    'open_price': dtype('float64'),
    'high_price': dtype('float64'),
    'low_price': dtype('float64'),
    'close_price': dtype('float64'),
    'volume_1': dtype('float64'),
    'volume_2': dtype('float64')
    }
"""