import pandas as pd
import numpy as np


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