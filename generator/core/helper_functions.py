import pandas as pd
import numpy as np
from core.constants import workers_mental_health_columns, workers_econ_effect_columns, preferred_empl_incentives_columns, preferred_fin_incentives_columns

def stopped_business_cond(no_business, tourism_business, non_tourism_business, job_status):
    status=[]
    for i in range(len(no_business)):
        value=np.nan
        if no_business[i]==1 and job_status[i]==1:
            value = 1
        elif (tourism_business[i]==1 and job_status[i]==2) or (no_business[i]==1 and job_status[i]==2) or (tourism_business[i]==1 and job_status[i]==1):
            value = 2
        elif (non_tourism_business[i]==1 and job_status[i]==3) or (no_business[i]==1 and job_status[i]==3) or (non_tourism_business[i]==1 and job_status[i]==1):
            value = 3
        elif (tourism_business[i]==1 and job_status[i]==3) or (non_tourism_business[i]==1 and job_status[i]==2) or (tourism_business[i]==1 and non_tourism_business[i]==1):
            value = 4
        status.append(value)
    return status
    

def workers_derived_variables(self_location, family_location):
    status = []
    for i in range(len(self_location)):
        value=np.nan
        if self_location[i]==4 and family_location[i] == 1:
            value=0
        else:
            value=1
        status.append(value)
    return status


def workers_derive_infection(self_infection, family_infection):
    status = []
    for i in range(len(self_infection)):
        value=np.nan
        if self_infection[i]==2 and family_infection[i]==2:
            value=0
        else:
            value=1
        status.append(value)
    return status


def business_derive_factors(raw_data, columns):
    status=[]
    for i in raw_data[columns].values:
        if np.isnan(i).all()==True:
            value=np.nan
        if 1 in i:
            value = 1
        else:
            value = 0
        status.append(value)
    return status


class PrepareData():
    def __init__(self, raw_data, survey):
        self.raw_data = raw_data
        self.survey = survey

    def prepare_business_data(self):
        self.raw_data
        self.raw_data['o_perm_stop_biz_start_new_biz_job'] = stopped_business_cond(self.raw_data['o_perm_stop_biz_start_new__1'], 
                                                                 self.raw_data['o_perm_stop_biz_start_new__2'],
                                                                 self.raw_data['o_perm_stop_biz_start_new__3'],
                                                                 self.raw_data['o_perm_stop_biz_start_new_job'])
        self.raw_data['i_covid_effect_business__10'] = business_derive_factors(self.raw_data, ['i_covid_effect_business__3', 'i_covid_effect_business__4', 
                                                                                    'i_covid_effect_business__8', 'i_covid_effect_business__10'])
        self.raw_data['i_covid_effect_business__6'] = business_derive_factors(self.raw_data, ['i_covid_effect_business__6', 'i_covid_effect_business__7'])
        self.raw_data['i_wrkfrc_actn_during_covid__1'] = business_derive_factors(self.raw_data, ['i_wrkfrc_actn_during_covid__1', 'i_wrkfrc_actn_during_covid__2'])
        self.raw_data['i_wrkfrc_actn_during_covid__5'] = business_derive_factors(self.raw_data, ['i_wrkfrc_actn_during_covid__5', 'i_wrkfrc_actn_during_covid__6'])
        self.raw_data['i_wrkfrc_actn_during_covid__7'] = business_derive_factors(self.raw_data, ['i_wrkfrc_actn_during_covid__7', 'i_wrkfrc_actn_during_covid__8'])
        self.raw_data['i_wrkfrc_actn_during_covid__9'] = business_derive_factors(self.raw_data, ['i_wrkfrc_actn_during_covid__9', 'i_wrkfrc_actn_during_covid__4'])
        self.raw_data['p_recvry_strategic_actions_internl__7'] = business_derive_factors(self.raw_data, ['p_recvry_strategic_actions_internl__7', 'p_recvry_strategic_actions_internl__2', 'p_recvry_strategic_actions_internl__6'])
        self.raw_data['p_recvry_strategic_actions_externl__8'] = business_derive_factors(self.raw_data, ['p_recvry_strategic_actions_externl__8', 'p_recvry_strategic_actions_externl__3', 'p_recvry_strategic_actions_externl__2'])
        self.raw_data['p_hlth_hhs_measures_1'] = business_derive_factors(self.raw_data, ['p_hlth_hhs_measures__1', 'p_hlth_hhs_measures__3'])
        self.raw_data['p_hlth_hhs_measures__9'] = business_derive_factors(self.raw_data, ['p_hlth_hhs_measures__9', 'p_hlth_hhs_measures__8', 'p_hlth_hhs_measures__6','p_hlth_hhs_measures__7'])
        self.raw_data['p_hlth_safety_measures__1'] = business_derive_factors(self.raw_data, ['p_hlth_safety_measures__1', 'p_hlth_safety_measures__3', 'p_hlth_safety_measures__6', 'p_hlth_safety_measures__7'])
        self.raw_data['p_hlth_safety_measures__2'] = business_derive_factors(self.raw_data, ['p_hlth_safety_measures__2', 'p_hlth_safety_measures__8'])
        self.raw_data['p_hlth_safety_measures__4'] = business_derive_factors(self.raw_data, ['p_hlth_safety_measures__4', 'p_hlth_safety_measures__5'])
        self.raw_data['n_rcvry_preferred_gov_policy__9'] = business_derive_factors(self.raw_data, ['n_rcvry_preferred_gov_policy__7', 'n_rcvry_preferred_gov_policy__8','n_rcvry_preferred_gov_policy__9'])
        self.raw_data['n_rcvry_preferred_gov_policy__3'] = business_derive_factors(self.raw_data, ['n_rcvry_preferred_gov_policy__3', 'n_rcvry_preferred_gov_policy__4'])
        self.raw_data['o_rcvry_biggest_support']
        self.raw_data.loc[self.raw_data['o_rcvry_biggest_support']==6, 'o_rcvry_biggest_support'] = 8
        self.raw_data.loc[self.raw_data['o_rcvry_biggest_support']==3, 'o_rcvry_biggest_support'] = 8

        return self.raw_data

    def prepare_workers_data(self):
        self.raw_data.rename(workers_mental_health_columns, axis=1, inplace=True)
        self.raw_data.rename(workers_econ_effect_columns, axis=1, inplace=True)
        self.raw_data.rename(preferred_empl_incentives_columns, axis=1, inplace=True)
        self.raw_data.rename(preferred_fin_incentives_columns, axis=1, inplace=True)
        binary_map = {1:0, 2: 1, None: None}
        for i in workers_mental_health_columns.values():
            self.raw_data[i] = self.raw_data[i].apply(func=lambda x: binary_map[x])
        self.raw_data['i_lvlhd_domicile_chng_self_fml'] = workers_derived_variables(self.raw_data['i_lvlhd_domicile_chng_self'], self.raw_data['i_lvlhd_domicile_chng_fml'])
        self.raw_data['i_hlth_covid_infectn_self_fml']= workers_derive_infection(self.raw_data['i_hlth_covid_infectn_self'], self.raw_data['i_hlth_covid_infectn_family'])
        self.raw_data['i_empl_covid_effects__1'] = business_derive_factors(self.raw_data, ['i_empl_covid_effects__1', 'i_empl_covid_effects__2'])
        self.raw_data['i_empl_covid_effects__6'] = business_derive_factors(self.raw_data, ['i_empl_covid_effects__6', 'i_empl_covid_effects__7'])
        self.raw_data['o_impct_to_self_nxt_6_mnths__1'] = business_derive_factors(self.raw_data, ['o_impct_to_self_nxt_6_mnths__1', 'o_impct_to_self_nxt_6_mnths__2'])
        return self.raw_data