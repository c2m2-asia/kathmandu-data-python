import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from core.univariate import Univariate
from core.bivariate import Bivariate


engine = create_engine('postgresql+psycopg2://{user}:{passwd}@{host}:{port}/{db}'.format(
        user='postgres',
        passwd=12345,
        host='localhost',
        port='5432',
        db='c2m2'))

rename_columns = {
                    'i_mental_hlth_think': 'i_mental_hlth__1', 
                    'i_mental_hlth_overconcerned': 'i_mental_hlth__2', 
                    'i_mental_hlth_detached': 'i_mental_hlth__3',
                    'i_mental_hlth_neg_think': 'i_mental_hlth__4',
                    'i_mental_hlth_blame': 'i_mental_hlth__5',
                    'i_mental_hlth_social': 'i_mental_hlth__6',
                    'i_mental_hlth_fml': 'i_mental_hlth__7'
                }


workers_raw_data = pd.read_excel('./raw/workers_data.xlsx')
workers_variable_map =  pd.read_excel('./data/generated_workers_variable_map.xlsx')
workers_labels_map = pd.read_excel('./raw/mapping_workers.xlsx')

workers_raw_data.rename(rename_columns, axis=1, inplace=True)
binary_map = {1:0, 2: 1, None: None}
for i in rename_columns.values():
    workers_raw_data[i] = workers_raw_data[i].apply(func=lambda x: binary_map[x])


workers_univariate = Univariate(raw_data=workers_raw_data, variable_map=workers_variable_map, labels_map=workers_labels_map)
workers_univariate_stats = workers_univariate.generate_univariate()
workers_univariate.generate_variable_map(workers_univariate_stats, 'workers')
workers_univariate_stats.to_sql('workers_univariate_stats', engine, index=False, if_exists='replace')

workers_bivariate = Bivariate(raw_data=workers_raw_data, variable_map=workers_variable_map, labels_map=workers_labels_map)
workers_bivariate_stats = workers_bivariate.generate_bivariate()
workers_bivariate_stats.to_sql('workers_bivariate_stats',  engine, index=False, if_exists='replace')