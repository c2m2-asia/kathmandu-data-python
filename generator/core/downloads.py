import pandas as pd
import numpy as np

def generate_download_data(raw_data, variable_label_map, variable_column_map):
    variable_column_map = variable_column_map[['RawDataColumns', 'DownloadsDataColumns', 'InputType', 'Group']]
    downloads_data = raw_data[variable_column_map['RawDataColumns']]
    variable_label_map = variable_label_map[['variable', 'value', 'label_en']]
    single_select_columns = variable_column_map[variable_column_map['InputType']=='single-select']['RawDataColumns']
    rename_dict = variable_column_map[['RawDataColumns', 'DownloadsDataColumns']].set_index('RawDataColumns').to_dict()["DownloadsDataColumns"]
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
    impact_columns = variable_column_map[variable_column_map['Group']=='Impact']['DownloadsDataColumns'].tolist()
    preparedness_columns = variable_column_map[variable_column_map['Group']=='Preparedness']['DownloadsDataColumns'].tolist()
    needs_columns = variable_column_map[variable_column_map['Group']=='Need']['DownloadsDataColumns'].tolist()
    outlook_columns = variable_column_map[variable_column_map['Group']=='Outlook']['DownloadsDataColumns'].tolist()
    general_columns = variable_column_map[variable_column_map['Group']=='general']['DownloadsDataColumns'].tolist()
    impact_downloads_data = downloads_data[general_columns + impact_columns]
    preparedness_downloads_data = downloads_data[general_columns + preparedness_columns]
    needs_downloads_data = downloads_data[general_columns + needs_columns]
    outlook_downloads_data = downloads_data[general_columns + outlook_columns]
    return impact_downloads_data, preparedness_downloads_data, needs_downloads_data, outlook_downloads_data



def fix_workforce_downloads(raw_data, labels_map):
    major_work_districts = [ i for i in raw_data.columns if 'trsm_major' in i]
    major_work_districts = raw_data[major_work_districts]
    labels_dict = labels_map[labels_map['variable'] == 'district'][['value', 'label_en']].set_index('value').to_dict()['label_en']
    labels_dict[0] = 0
    for i in major_work_districts.columns:
        major_work_districts[i] = major_work_districts[i].apply(func=lambda x: int(i.split('__')[1]) if x==1 else 0)
        major_work_districts[i] = major_work_districts[i].apply(func=lambda x: labels_dict[x])
    major_work_districts['trsm_major_work_district'] = major_work_districts.values.tolist()
    major_work_districts['trsm_major_work_district'] = major_work_districts['trsm_major_work_district'].apply(lambda x: [i for i in x if i!=0])
    major_work_districts['trsm_major_work_district'] = major_work_districts['trsm_major_work_district'].apply(lambda x: (', ').join(x))
    return major_work_districts['trsm_major_work_district']