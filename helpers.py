"""
Description:
This is the module containing the helpers that make petitions to
the weather API

Developed by: Jose D. Hernandez-Betancur

Date: March 5, 2023

Colapse columns source:
https://github.com/jamesshocking/collapse-spark-dataframe/blob/main/collapse_dataframe.py
"""

# Importing libraries
import os
from typing import Dict, List
import requests
from requests.exceptions import ReadTimeout, ConnectTimeout
from pyspark.sql.types import StructType
from pyspark.sql.functions import col


class WeatherAPI:
    """
    This is the class used to retrieve the weather data
    """

    def __init__(self):
        self.url = "https://weatherapi-com.p.rapidapi.com/current.json"
        self.headers = {"X-RapidAPI-Key": os.environ['RAPIDAPI_KEY'],
                        "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"}


    def get_weather_data(self, lat: float, lng: float) -> Dict:
        """
        Method to get the weather data per city name

        Args:
          - lat (float): The city latitude for the searching
          - lng (float): The city longitude for the searching
        Output:
          - result_dict (dictionary): Dictionary with the results
        """

        try:

            querystring = {"q": f"{lat},{lng}"}

            response = requests.request("GET", self.url,
                                        headers=self.headers,
                                        params=querystring,
                                        timeout=5)

            if response.status_code == 200:

                data = response.json()
                result_dict = {
                          'temp_c': data['current']['temp_c'],
                          'is_day': data['current']['is_day']
                        }
                result_dict.update({key: value for key, value in data['current'].items()
                              if key in ['wind_mph', 'wind_kph', 'wind_degree', 'wind_dir',
                                          'pressure_mb', 'pressure_in', 'precip_mm', 'precip_in',
                                          'humidity', 'cloud', 'feelslike_c', 'feelslike_f',
                                          'vis_km', 'vis_miles', 'uv', 'gust_mph', 'gust_kph']})

            else:

                result_dict = None

            return result_dict

        except (ReadTimeout, ConnectTimeout):

            return None


def get_all_columns_from_schema(source_schema: StructType) -> List:
    '''
    Function to get all columns that belong to the schema

    Agrs:
      - source_schema (StructType): schema for the dataframe

    Output
      - branches (list): List of columns
    '''

    branches = []
    def inner_get(schema: StructType, ancestor: List = None):
        """
        Function to analyze inner schema

        Args:
          - schema (StructType): dataframe schema
          - ancestor (list): list of ancerstors for nested schema
        """

        if ancestor is None:
            ancestor = []
        for field in schema.fields:
            branch_path = ancestor + [field.name]
            if isinstance(field.dataType, StructType):
                inner_get(field.dataType, branch_path)
            else:
                branches.append(branch_path)

    inner_get(source_schema)

    return branches


def collapse_columns(source_schema: StructType,
                  column_filter: str = None) -> List:
    """
    Function to collapse columns

    Args:
      - source_schema (StructType): schema for the dataframe
      - column_filter (string): Filter for columns

    Output:
      - _columns_to_select (list): List of columns for selecting
    """

    _columns_to_select = []
    if column_filter is None:
        column_filter = ""
    _all_columns = get_all_columns_from_schema(source_schema)
    for column_collection in _all_columns:
        if (len(column_filter) > 0) & (column_collection[0] != column_filter):
            continue

        select_column_collection = ['`%s`' % list_item for list_item in column_collection]

        if len(column_collection) > 1:
            _columns_to_select.append(
              col('.'.join(select_column_collection))
              .alias('_'.join(column_collection))
              )
        else:
            _columns_to_select.append(col(select_column_collection[0]))

    return _columns_to_select
