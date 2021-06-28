import pandas as pd
import numpy as np

def generate_download_data(raw_data, variable_label_map, variable_column_map):
    variable_column_map = variable_column_map[['PrevName', 'Name', 'InputType']]
    downloads_data = raw_data[variable_column_map['PrevName']]
    variable_label_map = variable_label_map[['variable', 'value', 'label_en']]
    single_select_columns = variable_column_map[variable_column_map['InputType']=='single-select']['PrevName']
    rename_dict = variable_column_map[['PrevName', 'Name']].set_index('PrevName').to_dict()["Name"]
    downloads_data['m_today'] = pd.to_datetime(downloads_data['m_today'])
    for i in single_select_columns:
        downloads_data[i].fillna(-1, inplace=True)
        downloads_data[i] = downloads_data[i].astype('int')
        if 'dist' in i:
            i_label_map = variable_label_map[variable_label_map['variable']=='district']
        elif 'rnk1' in i:
             i_label_map = variable_label_map[variable_label_map['variable']==i.replace('_rnk1', '')]
        elif 'rnk_1' in i:
            i_label_map = variable_label_map[variable_label_map['variable']==i.replace('_rnk_1', '')]
        else:
            i_label_map = variable_label_map[variable_label_map['variable']==i]
        i_label_map = i_label_map[['value', 'label_en']].set_index('value').to_dict()['label_en']
        i_label_map[-1] = np.nan
        downloads_data[i] = downloads_data[i].apply(lambda x: i_label_map[x])
    downloads_data.rename(columns = rename_dict, inplace=True)
    downloads_data.replace('Yes', '1', inplace=True)
    downloads_data.replace('No','0', inplace=True)
    return downloads_data