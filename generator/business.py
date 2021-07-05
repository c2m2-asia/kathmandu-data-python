import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from core.univariate import Univariate
from core.bivariate import Bivariate
from core.downloads import generate_download_data
from core.helper_functions import stopped_business_cond, business_derive_factors
pd.options.mode.chained_assignment = None


engine = create_engine('postgresql+psycopg2://{user}:{passwd}@{host}:{port}/{db}'.format(
        user='postgres',
        passwd=12345,
        host='localhost',
        port='5432',
        db='c2m2'))

business_variable_map =  pd.read_excel('./data/generated_business_variable_map.xlsx')
business_raw_data = pd.read_excel('./raw/business_data.xlsx')
business_raw_data.loc[business_raw_data['i_fin_savings_chng_2020_v_2019']==5, 'i_fin_savings_chng_2020_v_2019'] = 4
business_labels_map = pd.read_excel('./data/generated_business_labels_map.xlsx')
downloads_business_labels_map = pd.read_excel('./data/generated_business_labels_map_downloads.xlsx')
business_variable_column_map = pd.read_csv('./data/business_variable_column_map.csv')

business_impact_downloads_data, business_preparedness_downloads_data, business_needs_downloads_data, business_outlook_downloads_data, business_metadata_downloads_data = generate_download_data(
                                raw_data=business_raw_data, 
                                variable_label_map=downloads_business_labels_map,
                                variable_column_map=business_variable_column_map)
business_impact_downloads_data.to_sql('business_impact_downloads_data', engine, index=False, if_exists='replace')
business_preparedness_downloads_data.to_sql('business_preparedness_downloads_data', engine, index=False, if_exists='replace')
business_needs_downloads_data.to_sql('business_needs_downloads_data', engine, index=False, if_exists='replace')
business_outlook_downloads_data.to_sql('business_outlook_downloads_data', engine, index=False, if_exists='replace')
business_metadata_downloads_data.to_sql('business_metadata_downloads_data', engine, index=False, if_exists='replace')

business_raw_data['o_perm_stop_biz_start_new_biz_job'] = stopped_business_cond(business_raw_data['o_perm_stop_biz_start_new__1'], 
                                                                 business_raw_data['o_perm_stop_biz_start_new__2'],
                                                                 business_raw_data['o_perm_stop_biz_start_new__3'],
                                                                 business_raw_data['o_perm_stop_biz_start_new_job'])
business_raw_data['i_covid_effect_business__10'] = business_derive_factors(business_raw_data, ['i_covid_effect_business__3', 'i_covid_effect_business__4', 
                                                                            'i_covid_effect_business__8', 'i_covid_effect_business__10'])
business_raw_data['i_covid_effect_business__6'] = business_derive_factors(business_raw_data, ['i_covid_effect_business__6', 'i_covid_effect_business__7'])
business_raw_data['i_wrkfrc_actn_during_covid__1'] = business_derive_factors(business_raw_data, ['i_wrkfrc_actn_during_covid__1', 'i_wrkfrc_actn_during_covid__2'])
business_raw_data['i_wrkfrc_actn_during_covid__5'] = business_derive_factors(business_raw_data, ['i_wrkfrc_actn_during_covid__5', 'i_wrkfrc_actn_during_covid__6'])
business_raw_data['i_wrkfrc_actn_during_covid__7'] = business_derive_factors(business_raw_data, ['i_wrkfrc_actn_during_covid__7', 'i_wrkfrc_actn_during_covid__8'])
business_raw_data['i_wrkfrc_actn_during_covid__9'] = business_derive_factors(business_raw_data, ['i_wrkfrc_actn_during_covid__9', 'i_wrkfrc_actn_during_covid__4'])
business_raw_data['p_recvry_strategic_actions_internl__7'] = business_derive_factors(business_raw_data, ['p_recvry_strategic_actions_internl__7', 'p_recvry_strategic_actions_internl__2', 'p_recvry_strategic_actions_internl__6'])
business_raw_data['p_recvry_strategic_actions_externl__8'] = business_derive_factors(business_raw_data, ['p_recvry_strategic_actions_externl__8', 'p_recvry_strategic_actions_externl__3', 'p_recvry_strategic_actions_externl__2'])
business_raw_data['p_hlth_hhs_measures_1'] = business_derive_factors(business_raw_data, ['p_hlth_hhs_measures__1', 'p_hlth_hhs_measures__3'])
business_raw_data['p_hlth_hhs_measures__9'] = business_derive_factors(business_raw_data, ['p_hlth_hhs_measures__9', 'p_hlth_hhs_measures__8', 'p_hlth_hhs_measures__6'])
business_raw_data['p_hlth_safety_measures__1'] = business_derive_factors(business_raw_data, ['p_hlth_safety_measures__1', 'p_hlth_safety_measures__3', 'p_hlth_safety_measures__6', 'p_hlth_safety_measures__7'])
business_raw_data['p_hlth_safety_measures__2'] = business_derive_factors(business_raw_data, ['p_hlth_safety_measures__2', 'p_hlth_safety_measures__8'])
business_raw_data['p_hlth_safety_measures__4'] = business_derive_factors(business_raw_data, ['p_hlth_safety_measures__4', 'p_hlth_safety_measures__5'])
# business_raw_data['o_rcvry_biggest_support__8'] = business_derive_factors(business_raw_data, ['o_rcvry_biggest_support__3', 'o_rcvry_biggest_support__6','o_rcvry_biggest_support__8'])
business_raw_data['n_rcvry_preferred_gov_policy__9'] = business_derive_factors(business_raw_data, ['n_rcvry_preferred_gov_policy__7', 'n_rcvry_preferred_gov_policy__8','n_rcvry_preferred_gov_policy__9'])
business_raw_data['n_rcvry_preferred_gov_policy__3'] = business_derive_factors(business_raw_data, ['n_rcvry_preferred_gov_policy__3', 'n_rcvry_preferred_gov_policy__4'])
 

business_univariate = Univariate(raw_data=business_raw_data, variable_map=business_variable_map, labels_map=business_labels_map)
business_univariate_stats = business_univariate.generate_univariate()
business_univariate.generate_variable_map(business_univariate_stats, 'business')
business_univariate.labels_map.to_excel('./data/generated_business_labels_map.xlsx', index=False)
business_univariate_stats.drop('askedTotal', axis=1, inplace=True)
business_univariate_stats.columns = [ i.lower() for i in business_univariate_stats.columns]
business_univariate_stats.to_sql('businesses_univariate_stats ', engine, index=False, if_exists='replace')
# business_univariate_stats.to_csv('business_univariate_stats.csv', index=False)


business_bivariate = Bivariate(raw_data=business_raw_data, variable_map=business_variable_map, labels_map=business_labels_map)
business_bivariate_stats = business_bivariate.generate_bivariate()
business_bivariate_stats['total'] = business_bivariate_stats['total'].astype(int)
business_bivariate_stats['yValue'] = business_bivariate_stats['yValue'].astype(int)
business_bivariate_stats.columns = [ i.lower() for i in business_bivariate_stats.columns]
business_bivariate_stats.to_sql('businesses_bivariate_stats ',  engine, index=False, if_exists='replace')
business_bivariate_stats.to_csv('business_bivariate_stats.csv', index=False)
