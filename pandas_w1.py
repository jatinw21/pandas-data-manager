# to silent the pandas and numpy warnings that can be safely ignored
# https://github.com/ContinuumIO/anaconda-issues/issues/6678#issuecomment-337276215
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import sqlite3
from pandas.io import sql
import json
from pymongo import MongoClient
import requests


class PandasTester():
    '''
    Class testing the basic functionality of pandas
    '''
    def __init__(self, csvFile):
        self.testcsv = csvFile
        self.testdf = self.read_from_csv()
    
    def own_dataframe(self):
        '''
        Using python3 dicts as dataframes
        '''
        raw_data = {'first_name': ['Jason', 'Molly', 'Tina', 'Jake', 'Amy'],
                    'last_name': ['Miller', 'Jacobson', ".", 'Milner', 'Cooze'],
                    'age': [42, 52, 36, 24, 73],
                    'preTestScore': [4, 24, 31, ".", "."],
                    'postTestScore': ["25,000", "94,000", 57, 62, 70]}
        df = pd.DataFrame(raw_data)
        return df


    def read_from_csv(self, csvFile=None):
        '''
        Reading as csv into dataframe and printing
        '''
        # Reading from csv and converting into pandas dataframe
        # , name = ['A', 'B', 'C' ...] for column names
        csvFile = self.testcsv if not csvFile else csvFile

        df = pd.read_csv(csvFile)
        return df
        
    def specify_index_columns(self, indexCol, csvFile=None):
        '''
        Specifying custom primary key column
        '''
        csvFile = self.testcsv if not csvFile else csvFile

        # https://chrisalbon.com/python/data_wrangling/pandas_dataframe_importing_csv/
        # Makes 'JURISDICTION NAME' as the orimary key column and doesn't use the default 0,1,2...
        df = pd.read_csv(csvFile, index_col=indexCol)
        # Can also provide multiple columns as index columns by just providing a list of column names
        return df

    def cool_stuff_one(self, csvFile=None):
        '''
        We can specify what counts as missing values and then run do pd.isnull(df) to get true or false
        We can specify (..., headers=None) or not
        We can specify number of rows to skip using (..., skiprows=num)
        We can convert 56,000 to 56000 by using (..., thousands=',')
        '''
        csvFile = self.testcsv if not csvFile else csvFile

        # https://chrisalbon.com/python/data_wrangling/pandas_dataframe_importing_csv/

        # specify empty values
        sentinels = {'Last Name': ['.', 'NA'], 'Pre-Test Score': ['.']}
        # all cool features as params
        df = pd.read_csv(csvFile, na_values=sentinels, skiprows=3, thousands=',', headers=None)

        # get true/false for empty or non-empty values
        pd.isnull(df)
        print(df)

    def print_columns_headers(self, df=None):
        '''
        Print just the column headings
        '''
        df = self.testdf if not df else df
        # list(df.columns.values)
        # or 
        print(list(df))

    def print_rows_of_given_columns(self, columns, printHeadings=True, printIndex=False, df=None):
        '''
        print the specified rows
        '''
        df = self.testdf if not df else df

        if printIndex and printHeadings:
            print('INDEX', end=',')

        if printHeadings:
            print(",".join([columnHeader for columnHeader in columns]))

        for index, row in df.iterrows():
            if printIndex:
                print(index, end=' ')
            print(",".join([str(row[columnHeader]) for columnHeader in columns]))

    def save_df_to_csv(self, file_path, index=False, columns=None, df=None):
        '''
        Save a dataframe as a csv
        '''
        # default if not given
        df = self.testdf if not df else df

        # default is all columns if not given
        columns = list(df) if not columns else columns

        # write
        df.to_csv(file_path, sep=',', encoding='utf-8', columns=columns, index=False)

    def df_to_sqlite(self, db_path, df=None, tableName=None):
        '''
        Save dataframe as sqlite database table
        '''
        # default if not given
        df = self.testdf if not df else df
        tableName = 'demographics' if not tableName else tableName
        
        cnx = sqlite3.connect(db_path)
        sql.to_sql(df, tableName, cnx)

    def query_from_db_and_load(self, dbFile, tableName):
        '''
        Perform sample query from sqlite3 db to get all values 
        and return a dataframe
        '''
        cnx = sqlite3.connect(dbFile)
        return sql.read_sql('select * from ' + tableName, cnx)

    def write_in_mongodb(self, mongoHost, mongoPort, dbName, collection, df=None):
        '''
        Write df to a mongodb table
        '''
        # default if not given
        df = self.testdf if not df else df

        # establish connection
        client = MongoClient(mongoHost, mongoPort)

        # specify db name and table name
        db = client[dbName]
        c = db[collection]

        # You can only store documents in mongodb;
        # so you need to convert rows inside the dataframe into a list of json objects
        records = json.loads(df.T.to_json()).values()
        print(records)
        c.insert(records)

    def read_from_mongodb(self, mongoHost, mongoPort, dbName, collection):
        '''
        mongodb table to df
        '''
        client = MongoClient(host=mongoHost, port=mongoPort)
        db = client[dbName]
        c = db[collection]
        return pd.DataFrame(list(c.find()))

    def get_json_from_req(self, url):
        '''
        get request o the endpoint and get data as json
        '''
        resp = requests.get(url=url)
        data = resp.json()
        return data


    def json_to_dataframe(self, json_obj):
        """
        Please Open the JSON using the given URL to be familiar with the
        structure of the expected JSON object

        The root element contains two main elements : data and meta;
        the former contains the statistics for a given zip code, and
        the latter contains the information about the columns
        :param json_obj: JSON object for the dataset
        :return: A dataframe
        """
        # let's get the list of statistics for all zip codes
        json_data = json_obj['data']

        # to create a dataframe we also need the name of the columns:
        columns = []
        for c in json_obj['meta']['view']['columns']:
            columns.append(c['name'])

        return pd.DataFrame(data=json_data, columns=columns)

if __name__ == '__main__':
    csvFile = 'data/demographic.csv'
    db_path = 'data/app.db'
    mongoDbName = 'comp9321'
    mongoPort = 27018
    mongoHost = 'localhost'
    url = "https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.json"
    
    # create object and get test csv
    pdt = PandasTester(csvFile)

    # NOTE: raw data as dataframe testing
    # print(pdt.own_dataframe())

    # NOTE: reading from csv and print testing
    # print(pdt.read_from_csv())

    # NOTE: specify own primary key i.e. index column
    # print(pdt.specify_index_columns('JURISDICTION NAME'))

    # NOTE: We can test for missing values, specify rows to skip, turn off header,
    # remove commas from thousand and interpret it as numbers but not done here

    # NOTE: print a specific column of a dataframe
    # pdt.print_columns_headers()

    # NOTE: print rows of the dataframe
    # pdt.print_rows_of_given_columns(['JURISDICTION NAME', 'COUNT PARTICIPANTS'], True, True)

    # NOTE: Save df as csv
    # all columns with index
    # pdt.save_df_to_csv('./data/new_demo_allcols.csv', True)
    # 2 columns in opp order with index
    # pdt.save_df_to_csv('./data/new_demo_twocols.csv', True, ['COUNT PARTICIPANTS', 'JURISDICTION NAME'])

    # NOTE: save df as sqlite3 table
    # pdt.df_to_sqlite(db_path)
    # pdt.df_to_sqlite(db_path, None, 'mycustomtablename')

    # NOTE: REVIEW: All sql queries as pandas without using sqlite3
    # https://codeburst.io/how-to-rewrite-your-sql-queries-in-pandas-and-more-149d341fc53e

    # NOTE: Perform sample query from sqlite3 db to get all values as df
    # df = pdt.query_from_db_and_load(db_path, 'mycustomtablename')
    # print(df)

    # NOTE: df to mongodb table
    # pdt.write_in_mongodb(mongoHost, mongoPort, mongoDbName, 'Demographic_Statistics')

    # NOTE: mongodb table to df
    # print(pdt.read_from_mongodb(mongoHost, mongoPort, mongoDbName, 'Demographic_Statistics'))

    # NOTE: get request for json data
    # json_obj = pdt.get_json_from_req(url)
    # print(json_obj)
    # NOTE: json to dataframe
    # print(pdt.json_to_dataframe(json_obj))
