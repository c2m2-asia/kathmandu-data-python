import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from core.univariate import Univariate
from core.maps import Maps
from core.bivariate import Bivariate
from core.downloads import generate_download_data
from core.helper_functions import PrepareData
pd.options.mode.chained_assignment = None

# Connect to sql database 
engine = create_engine('postgresql+psycopg2://{user}:{passwd}@{host}:{port}/{db}'.format(
        user='postgres',
        passwd=12345,
        host='localhost',
        port='5432',
        db='c2m2'))

# Read necessary raw data and mapping files
business_variable_map =  pd.read_excel('./data/generated_business_variable_map.xlsx')
business_raw_data = pd.read_excel('./raw/business_data.xlsx')
business_raw_data.loc[business_raw_data['i_fin_savings_chng_2020_v_2019']==5, 'i_fin_savings_chng_2020_v_2019'] = 4
business_labels_map = pd.read_excel('./data/generated_business_labels_map.xlsx')
downloads_business_labels_map = pd.read_excel('./data/generated_business_labels_map_downloads.xlsx')
maps_business_labels_map = pd.read_excel('./data/generated_business_labels_map_maps.xlsx')
business_variable_column_map = pd.read_csv('./data/business_variable_column_map.csv')
maps_variable_map =  pd.read_excel('./data/map_viz_variable.xlsx')


#Prepare data for downloads section
business_impact_downloads_data, business_preparedness_downloads_data, business_needs_downloads_data, business_outlook_downloads_data = generate_download_data(
                                raw_data=business_raw_data, 
                                variable_label_map=downloads_business_labels_map,
                                variable_column_map=business_variable_column_map)
business_impact_downloads_data.to_csv('./data/downloads/business_impact_downloads_data.csv', index=False)
business_preparedness_downloads_data.to_csv('./data/downloads/business_preparedness_downloads_data.csv', index=False)
business_needs_downloads_data.to_csv('./data/downloads/business_need_downloads_data.csv', index=False)
business_outlook_downloads_data.to_csv('./data/downloads/business_outlook_downloads_data.csv', index=False)


# Prepare raw data with some derived variables and labels
prepare_data = PrepareData(business_raw_data, 'business')
business_raw_data = prepare_data.prepare_business_data()


# Data generation for maps based visualizations 
business_maps = Maps(raw_data=business_raw_data, variable_map=maps_variable_map, labels_map=maps_business_labels_map)
map_visualization_data = business_maps.generate_data()
map_visualization_data.to_sql('map_visualization_data',engine, index=False, if_exists='replace')
map_visualization_data.to_csv('map_visualization_data.csv', index=False)

# Merge business types for bivariate visualization 
business_raw_data['biz_type'] = 0
business_raw_data.loc[business_raw_data['m_biz_type']==1, 'biz_type']=4
business_raw_data.loc[business_raw_data['m_biz_type']==2, 'biz_type']=4
business_raw_data.loc[business_raw_data['m_biz_type']==3, 'biz_type']=3
business_raw_data.loc[business_raw_data['m_biz_type']==4, 'biz_type']=1
business_raw_data.loc[business_raw_data['m_biz_type']==5, 'biz_type']=2
business_raw_data.loc[business_raw_data['m_biz_type']==6, 'biz_type']=2
business_raw_data.loc[business_raw_data['m_biz_type']==7, 'biz_type']=2
business_raw_data.loc[business_raw_data['m_biz_type']==8, 'biz_type']=1
business_raw_data.loc[business_raw_data['m_biz_type']==9, 'biz_type']=9
business_raw_data['m_biz_type']= business_raw_data['biz_type']

# Data generation for Univariate Analysis
business_univariate = Univariate(raw_data=business_raw_data, variable_map=business_variable_map, labels_map=business_labels_map)
business_univariate_stats = business_univariate.generate_univariate()
business_univariate.generate_variable_map(business_univariate_stats, 'business')
business_univariate.labels_map.to_excel('./data/generated_business_labels_map.xlsx', index=False)
business_univariate_stats.drop('askedTotal', axis=1, inplace=True)
business_univariate_stats.columns = [ i.lower() for i in business_univariate_stats.columns]
business_univariate_stats.to_sql('businesses_univariate_stats', engine, index=False, if_exists='replace')
business_univariate_stats.to_csv('business_univariate_stats.csv', index=False)


# Data Generation For Bivariate Analysis
business_bivariate = Bivariate(raw_data=business_raw_data, variable_map=business_variable_map, labels_map=business_labels_map)
business_bivariate_stats = business_bivariate.generate_bivariate()
business_bivariate_stats['total'] = business_bivariate_stats['total'].astype(int)
business_bivariate_stats['yValue'] = business_bivariate_stats['yValue'].astype(int)
business_bivariate_stats.columns = [ i.lower() for i in business_bivariate_stats.columns]
business_bivariate_stats.to_sql('businesses_bivariate_stats',  engine, index=False, if_exists='replace')
business_bivariate_stats.to_csv('business_bivariate_stats.csv', index=False)
