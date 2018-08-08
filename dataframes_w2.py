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
from pandas_w1 import PandasTester
import re
import copy

class PandasMedium():
    '''
    Medium level functionality with dataframes
    '''
    def __init__(self, csvFile):
        self.pdt = PandasTester(csvFile)
        self.df = self.pdt.read_from_csv(csvFile)

    def print_dataframe(self, df):
        # print(self.df)
        # print(','.join(x for x in list(self.df)))
        for c in list(df):
            print('index, ', c)
            print(df[c])

    def print_all_columns_with_nan(self, csvFile):
        df = pd.read_csv(csvFile, na_values='NaN')
        emptydfs = pd.isnull(df)
        for c in list(emptydfs):
            print(c, end='\t')
            print(len(emptydfs[(emptydfs[c] == True)]))

    def better_print_nan(self, csvFile):
        df = pd.read_csv(csvFile, na_values='NaN')

        # NOTE: number of NaN in each column
        print(pd.isnull(df).sum()) # this works because True counts as 1 and then sum is taken
        # do .sum().sum() to find total over all values
        
        # NOTE: Total number of Nans anywhere
        print(pd.isnull(df).sum().sum())

    def fix_spaces_column_names(self, df):
        localdfColumns = copy.deepcopy(list(localdf))
        for c in localdfColumns:
            # print(c)
            temp = localdf[c]
            localdf[re.sub(r' ', '_', c)] = temp
            del localdf[c]

        return localdf


def fixPlace(s):
    # print(s)
    if 'London' in s:
        return 'London'
    return re.sub(r'-', ' ', s)

if __name__ == '__main__':
    csvFile = 'data/Books.csv'
    csvCity = 'data/City.csv'

    pdm = PandasMedium(csvFile)
    localdf = pdm.df
    # pdm.print_dataframe(localdf)

    # NOTE: Explore to see contents
    # print(localdf)
    # print(','.join(x for x in list(localdf)))
    # for c in list(localdf):
    #     print('index, ', c)
    #     print(localdf[c])

    # pdm.print_all_columns_with_nan(csvFile)

    # NOTE: number of NaN in each column
    # pdm.better_print_nan(csvFile)

    unwantedColumns = [
        'Edition Statement',
        'Corporate Author',
        'Corporate Contributors',
        'Former owner',
        'Engraver',
        'Contributors',
        'Issuance type',
        'Shelfmarks'
    ]

    # NOTE: drop required colummns
    # print(localdf)
    localdf = localdf.drop(columns=unwantedColumns)
    # doesnt work without columns=
    # otherwise we have to do localdf.drop(unwantedColumns, axis=1)
    # print(','.join(x for x in list(choppeddf)))
    # for c in list(choppeddf):
    #     print('index, ', c)
    #     print(choppeddf[c])

    # NOTE: Cleaning columns
    toFix = 'Place of Publication'

    # NOTE: applying a function to dataframe or column
    # print(localdf[toFix])

    # apply function to a single column by selecting the column
    # need to save it into the same column too coz pandas doesn't 
    # make changes to the dataframe provided, it returns them
    
    localdf[toFix] = localdf[toFix].apply(
        fixPlace)
    
    # check if changes applied
    # print(localdf[toFix])
    toFix = 'Date of Publication'
    # print(localdf[toFix])

    # NOTE: keep first 4 digits - using extract!!
    # Use captur groups to get required pattern out. It ignores NaN and keeps as is
    localdf[toFix] = localdf[toFix].str.extract(r'(\d\d\d\d)')

    # check if fixed
    # print(localdf[toFix])

    # NOTE: convert to numbers
    localdf[toFix] = pd.to_numeric(localdf[toFix], downcast='integer', errors='raise')

    # check if fixed
    # REVIEW: for some reason, pd.to_numeric is just converting to float and not integer
    # print(localdf[toFix])
    # but astype works for this as long as there are no NaN, so done it few lines below.

    # NOTE: replace NaN with 0
    # print(localdf[toFix])
    localdf[toFix] = localdf[toFix].fillna(0)

    # REVIEW: continued - another way to fix int
    localdf[toFix] = localdf[toFix].astype('int32')
    # print(localdf[toFix])

    # NOTE: using previously loaded data with cleansing. 
    # Replace the spaces in the column names with the underline character ('_')
    # Because panda's query method does not work well with column names which contains white spaces
    newLocaldf = pdm.fix_spaces_column_names(localdf)

    # check
    # print(','.join(x for x in list(newLocaldf)))

    # NOTE: Filter the rows and only keep books which are published in "London" after 1866
    # print(newLocaldf[(newLocaldf.Place_of_Publication == "London")
    #            & (newLocaldf.Date_of_Publication > 1866)])

    # NOTE: load city dataset
    citydf = pd.read_csv(csvCity)
    print(citydf)
    mergeddf = pd.merge(localdf, citydf, how='left', left_on=[
             'Place_of_Publication'], right_on=['City'])
    print(mergeddf)

    # index=False makes sure dataframe keeps name of countries and that is not set as index
    gb_country = mergeddf.groupby('Country', as_index=False)

    # the column chosen can be any, just to show count - works as long as that column has values for all
    # if the column isnt chosen then all columns will appear with same counts under each 
    # (except Country) which we grouped by
    count_df = gb_country['City'].count()
    print(count_df)

    

    


    
