#!/usr/bin/env python3
"""
@author: zhaoz
"""

import pandas as pd
import numpy as np
pd.set_option('max_row', None)
pd.set_option('max_column', None)


class MigrationDataReader():
    '''
    Methods that read, clean, and combine the between US-states migration data
    '''

    def __init__(self):
        self.state_names = ["Alaska", "Alabama", "Arkansas", "American Samoa", "Arizona", "California", "Colorado", "Connecticut", "District of Columbia", "Delaware", "Florida", "Georgia", "Guam", "Hawaii", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine", "Michigan", "Minnesota", "Missouri", "Mississippi",
            "Montana", "North Carolina", "North Dakota", "Nebraska", "New Hampshire", "New Jersey", "New Mexico", "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Virginia", "Virgin Islands", "Vermont", "Washington", "Wisconsin", "West Virginia", "Wyoming"]
        self.state_names = [s.lower() for s in self.state_names]
        self.population_year_range = range(2000, 2020)
        self.data_dir = 'data/'

    def read_data(self, fname, post2010, moe=False):
        '''
        data from 05-09 and from 10- are in different format, specify with post2010
        set moe to inclue Margin of Error estimates in the returned df
        '''
        df = pd.read_excel(
            fname,
            skiprows=range(0, 4)  # drop the disclaimers etc at the top
        )
        df.dropna(how='all', inplace=True)

        # drop columns that are repeated row indices
        # rep_cols = ['Current residence in' in c for c in df.columns]
        # df.drop(df.columns[rep_cols][1:],1,inplace=True)
        rep_index = df.columns[['Current' in s for s in df.iloc[0].fillna('')]][1:]
        df.drop(rep_index, 1, inplace=True)
        # drop footnotes(rows that are NAN in all columns other than the first)
        df.dropna(thresh=2, inplace=True)
        # drop replicate rows with column names in the middle of the body
        df.drop_duplicates(inplace=True)

        if post2010:
            # df.iloc[1,0:7] = df.iloc[0,0:7]
            # remove the col 1-7 since they were not included in older datasets
            df.drop(df.columns[1:7], 1, inplace=True)
        # fill in to-be column names
        for c in np.arange(1, df.shape[1]):
            if pd.isnull(df.iloc[1, c]):
                df.iloc[1, c] = df.iloc[1, c-1]
        # change column names and remove extra index rows
        df.columns = df.iloc[1].fillna('') + '_' + df.iloc[2].fillna('')
        df.columns.values[0] = 'to'
        df.drop([0, 1, 2], 0, inplace=True)
        # change to long format
        df = df.melt(id_vars='to')
        df[['from', 'type']] = df['variable'].str.lower().str.split('_', expand=True)
        df.drop('variable', 1, inplace=True)
        # state names to lower
        df['to'] = df['to'].str.lower()
        df['from'] = df['from'].str.lower()

        if moe:
            return df
        else:
            df = df[df['type'] == 'estimate']
            df.rename(columns={'value': 'estimate'}, inplace=True)
            df = df.drop(columns='type')
            # a few post 2010 auto-transitions are coded as weird strings, drop
            if post2010:
                # some kink in the data file
                # to string, remove weird string looking thing, later to int solves it
                df.loc[df['estimate'].astype('str').str.contains(
                    'N/A', na=False), 'estimate'] = np.nan
                df.dropna(axis=0, inplace=True)
            # change the estimates to int
            df['estimate'] = df['estimate'].astype('int')
            return df

    def read_population_single_file(self, fname):
        df = pd.read_excel(
            fname,
            header=3
        )
        df.dropna(how='all', inplace=True)
        # remove the leading '.'s in the state name column
        df.iloc[:, 0] = df.iloc[:, 0].str.replace('.', '')
        df.rename({'Unnamed: 0': 'state'}, axis=1, inplace=True)
        sel_col = ['state'] + list(self.population_year_range)
        # select the single-year columns and single-state rows
        df['state'] = df['state'].str.lower()
        df = df.loc[df['state'].isin(self.state_names),
                    df.columns.isin(sel_col)]
        # convert to long formate
        df = df.melt(id_vars='state', var_name='year', value_name='pop')
        return df

    def read_population_data(self):
        '''
        read state level population data across all years
        '''
        f1 = self.data_dir + 'state_pop_tot_00-09.xls'
        f2 = self.data_dir + 'state_pop_tot_10-19.xlsx'
        df = pd.concat((self.read_population_single_file(f1),
                        self.read_population_single_file(f2)))
        return(df)

    def read_data_year_range(self, first=2005, last=2019, population=True, moe=False):
        # read migration data within year range
        for year in range(first, last+1):
            fname = f'{self.data_dir}state_to_state_migrations_table_{year}.xls'
            post2010 = year >= 2010
            df = self.read_data(fname, post2010, moe)
            df['year'] = year
            if year == first:
                df_all = df
            else:
                df_all = pd.concat((df_all, df))
        # read population data and join, if population True
        if population:
            df_pop = self.read_population_data()
            df_all['year_lag_1'] = df_all['year'] - 1
            df_all = df_all.merge(
                df_pop, how='left',
                left_on=['from', 'year_lag_1'],
                right_on=['state', 'year']
            ).rename(
                {'pop': 'pop_from_lag1'}, axis=1
            ).merge(
                df_pop, how='left',
                left_on=['to', 'year_x'],
                right_on=['state', 'year']
            ).rename({'pop': 'pop_to'}, axis=1)
            # drop unnecessary columns generated from merge
            df_all.drop('year_lag_1',1,inplace = True)
            df_all.drop(df_all.columns[df_all.columns.str.contains('_x')|df_all.columns.str.contains('_y')],
                        axis = 1, inplace = True)
        return df_all

    def extra_cleaning(self, df):
        # keep only actual us state data (+ dc)
        df = df[df['from'].isin(self.state_names) & df['to'].isin(self.state_names)]
        # older datasets encoded stay-in-state as auto transitions, remove
        df = df[df['to'] != df['from']]
        return df

if __name__ == '__main__':
    reader = MigrationDataReader()
    df = reader.read_data_year_range().head()
    df = reader.extra_cleaning(df)
    print(df.head())
