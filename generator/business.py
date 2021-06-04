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

business_variable_map =  pd.read_excel('./data/generated_business_variable_map.xlsx')
business_raw_data = pd.read_excel('./raw/business_data.xlsx')
business_raw_data.loc[business_raw_data['i_fin_savings_chng_2020_v_2019']==5, 'i_fin_savings_chng_2020_v_2019'] = 4
business_labels_map = pd.read_excel('./raw/mapping_business.xlsx')



business_univariate = Univariate(raw_data=business_raw_data, variable_map=business_variable_map, labels_map=business_labels_map)
business_univariate_stats = business_univariate.generate_univariate()
business_univariate.generate_variable_map(business_univariate_stats, 'business')
business_univariate_stats.to_sql('business_univariate_stats', engine, index=False, if_exists='replace')


business_bivariate = Bivariate(raw_data=business_raw_data, variable_map=business_variable_map, labels_map=business_labels_map)
business_bivariate_stats = business_bivariate.generate_bivariate()
business_bivariate_stats.to_sql('business_bivariate_stats',  engine, index=False, if_exists='replace')