import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from core.univariate import Univariate
from core.bivariate import Bivariate
from core.downloads import generate_download_data
from core.helper_functions import workers_derived_variables, workers_derive_infection
from core.constants import workers_mental_health_columns, workers_econ_effect_columns
pd.options.mode.chained_assignment = None


engine = create_engine('postgresql+psycopg2://{user}:{passwd}@{host}:{port}/{db}'.format(
        user='postgres',
        passwd=12345,
        host='localhost',
        port='5432',
        db='c2m2'))


workers_raw_data = pd.read_excel('./raw/workers_data.xlsx')
workers_variable_map =  pd.read_excel('./data/generated_workers_variable_map.xlsx')
workers_labels_map = pd.read_excel('./data/generated_workers_labels_map.xlsx')
workers_variable_column_map = pd.read_csv('./data/workforce_variable_column_map.csv')

workers_impact_downloads_data, workers_preparedness_downloads_data, workers_needs_downloads_data, workers_outlook_downloads_data, workers_metadata_downloads_data = generate_download_data(
                                raw_data=workers_raw_data, 
                                variable_label_map=workers_labels_map,
                                variable_column_map=workers_variable_column_map)
workers_impact_downloads_data.to_sql('workers_impact_downloads_data', engine, index=False, if_exists='replace')
workers_preparedness_downloads_data.to_sql('workers_preparedness_downloads_data', engine, index=False, if_exists='replace')
workers_needs_downloads_data.to_sql('workers_needs_downloads_data', engine, index=False, if_exists='replace')
workers_outlook_downloads_data.to_sql('workers_impact_downloads_data', engine, index=False, if_exists='replace')
workers_metadata_downloads_data.to_sql('workers_impact_downloads_data', engine, index=False, if_exists='replace')

workers_raw_data.rename(workers_mental_health_columns, axis=1, inplace=True)
workers_raw_data.rename(workers_econ_effect_columns, axis=1, inplace=True)
binary_map = {1:0, 2: 1, None: None}
for i in workers_mental_health_columns.values():
    workers_raw_data[i] = workers_raw_data[i].apply(func=lambda x: binary_map[x])

workers_raw_data['i_lvlhd_domicile_chng_self_fml'] = workers_derived_variables(workers_raw_data['i_lvlhd_domicile_chng_self'], workers_raw_data['i_lvlhd_domicile_chng_fml'])
workers_raw_data['i_hlth_covid_infectn_self_fml']= workers_derive_infection(workers_raw_data['i_hlth_covid_infectn_self'], workers_raw_data['i_hlth_covid_infectn_family'])
workers_univariate = Univariate(raw_data=workers_raw_data, variable_map=workers_variable_map, labels_map=workers_labels_map)
workers_univariate_stats = workers_univariate.generate_univariate()
workers_univariate.generate_variable_map(workers_univariate_stats, 'workers')
workers_univariate.labels_map.to_excel('./data/generated_workers_labels_map.xlsx', index=False)
workers_univariate_stats.drop('askedTotal', axis=1, inplace=True)
workers_univariate_stats.columns = [ i.lower() for i in workers_univariate_stats.columns]
workers_univariate_stats.to_sql('workers_univariate_stats', engine, index=False, if_exists='replace')
# workers_univariate_stats.to_csv('workers_univariate_stats.csv', index=False)

workers_bivariate = Bivariate(raw_data=workers_raw_data, variable_map=workers_variable_map, labels_map=workers_labels_map)
workers_bivariate_stats = workers_bivariate.generate_bivariate()
workers_bivariate_stats.columns = [ i.lower() for i in workers_bivariate_stats.columns]
workers_bivariate_stats.to_sql('workers_bivariate_stats',  engine, index=False, if_exists='replace')
# workers_bivariate_stats.to_csv('workers_bivariate_stats.csv', index=False)
