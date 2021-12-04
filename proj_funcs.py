import os.path
import requests
import time
import urllib

import pandas as pd
from sqlalchemy import create_engine

def getTradingData(eth_bitstamp_urls: list):
    """Downloads the trading data based on a list of URLs.

    Args:
        eth_bitstamp_urls (list): list of URLs
    """

    for url in eth_bitstamp_urls:

        file_name = url.split("Bitstamp_")[-1]
        if not os.path.isfile("eth-usd/" + file_name):
            urllib.request.urlretrieve(url, "eth-usd/" + file_name)

    pass

def describeTradingData(path: str, sk_rows:int = 0):
    """Describes a given dataframe via descriptive statistics.

    Args:
        path (str): where the CSVs are stored, relative from the
                    folder where the code is being executed.
        sk_rows (int, optional): the number of lines to skip in
                    the CSVs. Defaults to 0.
    """

    for file in os.listdir(path):
        print("****" +file+ "****")
        eth_data = pd.read_csv("eth-usd/" + file, skiprows = sk_rows)
        desc_data = eth_data.describe(include='all')
        desc_null = eth_data.isnull().sum().to_frame(name = 'missing').T
        print(pd.concat([desc_data, desc_null]))
        print("   ")

    pass

def checkDataTypes(data: pd.DataFrame, data_types: list) -> bool:
    """Check if a dataframe has the data types of a given list.

    Args:
        data (pd.DataFrame): the dataframe to perform the check onto.
        data_types (list): the datatypes expected from the dataframe's
                            columns.

    Returns:
        bool: True if test is passed, False otherwise.
    """
    columns = list(data.columns)
    check = True

    try:
        assert all([data[columns[i]].dtype == data_types[i] \
            for i in range(len(columns))]), \
            "Data types must match expected ones, as per data_types list."
        print("Data types check OK!")

    except Exception as e:
        print("Error: " + str(e))
        check = False

    finally:
        return check

def checkUniqueVal(data: pd.DataFrame, col: str, unique_val) -> bool:
    """Check if a dataframe's column's unique value is equal to a given value.

    Args:
        data (pd.DataFrame): the dataframe to perform the check onto.
        col (str): the column to perform the check onto.
        unique_val ([type]): the value the colum's unique value should be
                                equal to.

    Returns:
        bool: True if test is passed, False otherwise.
    """
    check = True
    try:
        assert data[col].unique() == [unique_val], \
            "Unique value for the " + col + " column should be " + \
                str(unique_val)
        print("Unique value check for column '" + col + "' OK!")

    except Exception as e:
        print("Error: " + str(e))
        check = False

    finally:
        return check

def checkMaxUnixTimestamp(data: pd.DataFrame, col: str) -> bool:
    """Check if a dataframe's column's timestamp values
        are older than now.

    Args:
        data (pd.DataFrame): the dataframe to perform the check onto.
        col (str): the column to perform the check onto.

    Returns:
        bool: True if test is passed, False otherwise.
    """
    check = True
    try:
        assert int(time.time()) >= data[col].max(), \
            "The timestamp of the column " + col + \
            " can't be greater than now's timestamp."
        print("Unix Timestamp value check for column '" + col + "' OK!")

    except Exception as e:
        print("Error: " + str(e))
        check = False

    finally:
        return check

def checkUnixWithTimeCols(
                        data: pd.DataFrame,
                        unix_col: str,
                        time_col: str) -> bool:
    """Check if the time represented by a unix timestamp column is the same as
        the time of a datetime column

    Args:
        data (pd.DataFrame): the dataframe to perform the check onto.
        unix_col (str): unix timestamp column name.
        time_col (str): datetime timestamp column name.

    Returns:
        bool: True if test is passed, False otherwise.
    """
    check = True
    try:
        assert all(data[unix_col] == data[time_col]), \
            "Unix timestamp of col " + unix_col + \
            " must correspond to datetime timestamp of column " + \
            time_col +  "."

        print("Unix Timestamp value check for columns '" + unix_col + \
                "' and '" + time_col + "' OK!")

    except Exception as e:
        print("Error: " + str(e))
        check = False

    finally:
        return check


def checkDiffEqualTo(
            data: pd.DataFrame,
            diff_col: str,
            diff_val,
            ) -> bool:
    """Checks if the difference between a given value and the previous are
    equal to a given value in a given column.

    Args:
        data (pd.DataFrame): the dataframe to perform the check onto.
        diff_col (str): the column to perform the check onto.
        diff_val (str): the value the differences should be equal to

    Returns:
        bool: True if test is passed, False otherwise.
    """

    check = True
    try:
        assert all(data[diff_col][1:data.shape[0]-1] == diff_val), \
        "The difference between two consecutive data rows should be " + \
            str(diff_val)+ " for column " + str(diff_col) +  "."

        print("Difference between values in the " + str(diff_col) + \
                "column equal to " + str(diff_val) + ": OK!")

    except Exception as e:
        print("Error: " + str(e))
        check = False

    finally:
        return check

def checkDiffGreaterEqualTo(
            data: pd.DataFrame,
            diff_col: str,
            diff_val,
            ) -> bool:
    """Checks if the difference between a given value and the previous are
    greater or equal to a given value in a given column.

    Args:
        data (pd.DataFrame): the dataframe to perform the check onto.
        diff_col (str): the column to perform the check onto.
        diff_val (str): the value the differences should be equal to

    Returns:
        bool: True if test is passed, False otherwise.
    """

    check = True
    try:
        assert all(data[diff_col][1:data.shape[0]-1] >= diff_val), \
        "The difference between two consecutive data rows should be " + \
            str(diff_val)+ " for column " + str(diff_col) +  "."

        print("Difference between values in the " + str(diff_col) + \
                " column equal to " + str(diff_val) + ": OK!")

    except Exception as e:
        print("Error: " + str(e))
        check = False

    finally:
        return check

def persistToDb(endpoint: str, query: str, data: pd.DataFrame,
                table: str, table_schema: str):

    engine = create_engine(endpoint)
    with engine.connect() as con:
        con.execute(query)
        data.to_sql(
                    table,
                    con,
                    schema = table_schema,
                    if_exists = 'append',
                    index = False
                    )

def getEtherScanResults(start_date:str, end_date:str,
                        api_key:str, url_template:str):

  response = requests.get(url_template.format(start_date, end_date, api_key))
  if response.status_code == 200:
      return pd.json_normalize(response.json()['result'])
  else:
      return "invalid response"