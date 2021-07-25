import numpy as np
import pandas as pd


class Maps():
    def __init__(self, raw_data, variable_map, labels_map):
        self.raw_data = raw_data
        self.raw_data = self.raw_data[self.raw_data['m_coodinates']!=' ']
        self.variable_map = variable_map
        self.labels_map = labels_map
        self.selected_variables = self.variable_map[self.variable_map['selected']=='yes']
        
    
    def generate_data(self):
        s_select_data = self.generate_s_select()
        m_select_data= self.generate_multiselect()
        sorted_m_select_data = []
        for i in m_select_data['variable'].unique():
            each_var_df = m_select_data[m_select_data['variable']==i]
            each_var_df.sort_values('labelIndex', ascending=False, inplace=True)
            if len(sorted_m_select_data)==0:
                sorted_m_select_data = each_var_df
            else:
                sorted_m_select_data = pd.concat((sorted_m_select_data, each_var_df), axis=0)
        map_viz_df = pd.concat((s_select_data, sorted_m_select_data), axis=0)
        map_viz_df['m_today'] = pd.to_datetime(map_viz_df['m_today'])
        map_viz_df['m_today'] = self.map_date(map_viz_df['m_today'])
        map_viz_df['label_en'] = map_viz_df['variable_value'].apply(func=lambda x: self.labels_map[self.labels_map['variableLabel']==x]['label_en'].tolist()[0])
        map_viz_df['label_ne'] = map_viz_df['variable_value'].apply(func=lambda x: self.labels_map[self.labels_map['variableLabel']==x]['label_ne'].tolist()[0])
        map_viz_df['m_biz_type'] = map_viz_df['m_biz_type'].apply(func=lambda x: 'm_biz_type__'+str(x))
        map_viz_df['m_biz_type'] = map_viz_df['m_biz_type'].apply(func= lambda x: self.labels_map[self.labels_map['variableLabel']==x]['label_en'].tolist()[0])
        map_viz_df['latitude'] = map_viz_df['coordinates'].apply(func=lambda x: x[0])
        map_viz_df['longitude'] = map_viz_df['coordinates'].apply(func=lambda x: x[1])
        map_viz_df['index'] = range(1,len(map_viz_df)+1)
        map_viz_df.drop(['variable_value', 'coordinates', 'labelIndex'], axis=1, inplace=True)
        map_viz_df.columns = ['businessname', 'submissiondate', 'businesstype', 'variable', 'value',
                   'percoftotal', 'total', 'label_en', 'label_ne', 'latitude','longitude', 'index']
        return map_viz_df
    
    
    def generate_s_select(self):
        s_select_variables = self.selected_variables[self.selected_variables['input_type']=='single-select']['variable'].tolist()
        s_select_df = []
        for i in s_select_variables:
            each_var_df = self.raw_data[['m_name_business', 'm_today', 'm_coodinates', 'm_biz_type']]
            each_var_df['coordinates'] = each_var_df['m_coodinates'].apply(func= lambda x: (float(x.split(' ')[0]), float(x.split(' ')[1])))
            each_var_df.drop('m_coodinates', axis=1, inplace=True)
            each_var_df['variable'] = i
            each_var_df['value'] = self.raw_data[i]
            each_var_df.dropna(inplace=True)
            each_var_df['value'] = each_var_df['value'].astype(int)
            each_var_df['variable_value'] = each_var_df['value'].apply(func=lambda x: i+'__'+str(x))
            percentage_dict = np.round(each_var_df['value'].value_counts()/len(each_var_df)*100, 0).to_dict()
            total_dict = each_var_df['value'].value_counts().to_dict()
            each_var_df['perc_of_total'] = each_var_df['value'].apply(lambda x: percentage_dict[x]).astype(int)
            each_var_df['total'] = each_var_df['value'].apply(lambda x: total_dict[x])
            each_var_df['labelIndex'] = each_var_df['variable_value'].apply(lambda x: self.labels_map[self.labels_map['variableLabel']==x]['labelIndex'].tolist()[0])
            each_var_df.sort_values('labelIndex', inplace=True, ascending=False)
            if len(s_select_df)==0:
                s_select_df = each_var_df
            else:
                s_select_df = pd.concat((s_select_df, each_var_df), axis=0)
        return s_select_df
    
    def generate_multiselect(self):
        m_select_df = []
        m_select_variables = self.selected_variables[self.selected_variables['input_type']=='multi-select']['variable'].tolist()
        m_select_variables = self.labels_map.set_index('variable').transpose()[m_select_variables]
        m_select_variables = m_select_variables.transpose().reset_index()
        m_select_variables = m_select_variables['variableLabel'].tolist()
        for i in m_select_variables:
            each_var_df = self.raw_data[['m_name_business', 'm_today', 'm_coodinates', 'm_biz_type']]
            each_var_df['coordinates'] = each_var_df['m_coodinates'].apply(func= lambda x: (float(x.split(' ')[0]), float(x.split(' ')[1])))
            each_var_df.drop('m_coodinates', axis=1, inplace=True)
            each_var_df['variable'] = i.split('__')[0]
            each_var_df['value'] = self.raw_data[i]
            each_var_df['value'] = self.set_value(each_var_df['value'], i)
            each_var_df.dropna(inplace=True)
            variable_label = {}
            variable_label[i.split('__')[0]] = list(self.labels_map[self.labels_map['variable']==i.split('__')[0]]['variableLabel'])
            asked_total = 0
            for j in self.raw_data[variable_label[i.split('__')[0]]].values:
                if (1 in j) or (0 in j):
                    asked_total +=1
            each_var_df['value'] = each_var_df['value'].astype(int)
            each_var_df['variable_value'] = i
            each_var_df['total'] = len(each_var_df)
            each_var_df['perc_of_total'] = np.round((each_var_df['total']/asked_total)*100, 0)
            each_var_df['labelIndex'] = each_var_df['variable_value'].apply(lambda x: self.labels_map[self.labels_map['variableLabel']==x]['labelIndex'].tolist()[0])
            if len(m_select_df)==0:
                m_select_df = each_var_df
            else:
                m_select_df = pd.concat((m_select_df, each_var_df), axis=0)
        return m_select_df
                
    def set_value(self, values, variable):
        output =[]
        for value in values:
            if value==1:
                output.append(variable.split('__')[1])
            else:
                output.append(np.nan)
        return output
        
    
    def map_date(self, date_column):
        output = []
        for date in date_column:
            if date <= pd.to_datetime('2021-04-28'):
                output.append(1)
            elif (date > pd.to_datetime('2021-04-28')):
                output.append(2)
        return output