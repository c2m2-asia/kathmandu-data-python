import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from core.univariate import Univariate
from core.bivariate import Bivariate
from core.downloads import generate_download_data, fix_workforce_downloads
from core.helper_functions import workers_derived_variables, workers_derive_infection, PrepareData
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
downloads_workers_labels_map = pd.read_excel('./data/generated_workers_labels_map_downloads.xlsx')

#Prepare data for downloads section
workers_impact_downloads_data, workers_preparedness_downloads_data, workers_needs_downloads_data, workers_outlook_downloads_data = generate_download_data(
                                raw_data=workers_raw_data, 
                                variable_label_map=downloads_workers_labels_map,
                                variable_column_map=workers_variable_column_map)
trsm_major_work_district = fix_workforce_downloads(workers_raw_data, workers_labels_map)
workers_impact_downloads_data['trsm_major_work_district'] = trsm_major_work_district
workers_preparedness_downloads_data['trsm_major_work_district'] = trsm_major_work_district
workers_needs_downloads_data['trsm_major_work_district'] = trsm_major_work_district
workers_outlook_downloads_data['trsm_major_work_district'] = trsm_major_work_district

workers_impact_downloads_data.to_csv('./data/downloads/workers_impact_downloads_data.csv', index=False)
workers_preparedness_downloads_data.to_csv('./data/downloads/workers_preparedness_downloads_data.csv', index=False)
workers_needs_downloads_data.to_csv('./data/downloads/workers_need_downloads_data.csv', index=False)
workers_outlook_downloads_data.to_csv('./data/downloads/workers_outlook_downloads_data.csv', index=False)


# Prepare raw data with some derived variables and labels
prepare_data = PrepareData(workers_raw_data, 'workforce')
workers_raw_data = prepare_data.prepare_workers_data()


# Data generation for Univariate Analysis
workers_univariate = Univariate(raw_data=workers_raw_data, variable_map=workers_variable_map, labels_map=workers_labels_map)
workers_univariate_stats = workers_univariate.generate_univariate()
workers_univariate.generate_variable_map(workers_univariate_stats, 'workers')
workers_univariate.labels_map.to_excel('./data/generated_workers_labels_map.xlsx', index=False)
workers_univariate_stats.drop('askedTotal', axis=1, inplace=True)
workers_univariate_stats.columns = [ i.lower() for i in workers_univariate_stats.columns]
workers_univariate_stats.to_sql('workers_univariate_stats', engine, index=False, if_exists='replace')
# workers_univariate_stats.to_csv('workers_univariate_stats.csv', index=False)


#Data Generation For Bivariate Analysis
workers_bivariate = Bivariate(raw_data=workers_raw_data, variable_map=workers_variable_map, labels_map=workers_labels_map)
workers_bivariate_stats = workers_bivariate.generate_bivariate()
workers_bivariate_stats.columns = [ i.lower() for i in workers_bivariate_stats.columns]
workers_bivariate_stats.to_sql('workers_bivariate_stats',  engine, index=False, if_exists='replace')
# workers_bivariate_stats.to_csv('workers_bivariate_stats.csv', index=False)
