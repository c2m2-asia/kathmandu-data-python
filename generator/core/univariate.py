import numpy as np
import pandas as pd

class Univariate():
    def __init__(self, raw_data, variable_map, labels_map):
        self.raw_data = raw_data
        self.variable_map = variable_map
        self.labels_map = labels_map
        self.selected_variables = self.variable_map[self.variable_map['selected'] ==1]
        self.s_select_variables = self.selected_variables[self.selected_variables['input_type'] == 'single-select']['variable'].values.tolist()
        self.groupby_columns = self.selected_variables[self.selected_variables['input_type'] == 'groupby']['variable'].values.tolist()
        self.s_select_variables = self.s_select_variables + self.groupby_columns
        self.m_select_variables = self.selected_variables[self.selected_variables['input_type'] == 'multi-select']['variable'].values.tolist()
    
    def generate_univariate(self):
        univariate_stats = pd.concat([self.generate_singleselect(),self.generate_multiselect()])
        univariate_stats.drop('variable_label', axis=1, inplace=True)
        univariate_stats['index'] = list(range(1, len(univariate_stats)+1))
        univariate_stats.drop('choice_code', axis=1, inplace=True)
        queue_dict = self.variable_map[['variable', 'queue_index']].set_index('variable').to_dict()['queue_index']
        univariate_stats['queue_index'] = univariate_stats['variable'].apply(lambda x: queue_dict[x])
        univariate_stats.sort_values(by='queue_index', inplace=True)
        univariate_stats.drop('queue_index', axis=1, inplace=True)
        return univariate_stats
    
    
    def generate_singleselect(self):
        self.initiallize_columns()
        s_select_univariate = self.labels_map.set_index('variable').transpose()[self.s_select_variables]
        s_select_univariate = s_select_univariate.transpose().reset_index()
        self.s_select_generate_total(s_select_univariate)
        s_select_univariate['perc_of_total'] = s_select_univariate['total'] / s_select_univariate['asked_total']
        s_select_univariate['total'] = s_select_univariate['total'].astype(int)
        s_select_univariate['perc_of_total'].fillna(0, inplace=True)
        return s_select_univariate
    
    def generate_multiselect(self):
        self.initiallize_columns()
        m_select_univariate = self.labels_map.set_index('variable').transpose()[self.m_select_variables]
        m_select_univariate = m_select_univariate.transpose().reset_index()
        self.m_select_generate_total(m_select_univariate)
        m_select_univariate['perc_of_total'] = m_select_univariate['total'] / m_select_univariate['asked_total']
        m_select_univariate['total'] = m_select_univariate['total'].astype(int)
        m_select_univariate['perc_of_total'].fillna(0, inplace=True)
        return m_select_univariate    
    
    def initiallize_columns(self):
        self.labels_map[['total', 'perc_of_total', 'asked_total']] = 0
        self.labels_map['universe'] = self.raw_data.shape[0]
        self.labels_map['variable_label'] = self.variable_value(self.labels_map['variable'], self.labels_map['value'])
        
    def s_select_generate_total(self, df):
        count_dict = {}
        for i in set(df['variable']):
            label_counts = pd.DataFrame(self.raw_data[i].value_counts()).reset_index()
            label_counts = label_counts.astype(int)
            label_counts = label_counts.set_index('index').to_dict().values()
            count_dict[i] = list(label_counts)[0]
        for i in list(df['variable_label']):
            variable = i.split('__')[0]
            value = int(i.split('__')[1])
            try:
                total = count_dict[variable][value]
            except:
                total = 0
            asked_total = sum(list(count_dict[variable].values()))
            df.loc[df['variable_label'] == i, ['total', 'asked_total']] = total, asked_total
                
    def m_select_generate_total(self, df):
        label_dict = {}
        for i in df['variable']:
            label_dict[i] = list(df[df['variable']==i]['variable_label'])
        count_dict = {}
        for i in label_dict.keys():
            count = 0
            for j in self.raw_data[label_dict[i]].values:
                if 1 in j:
                    count +=1
            count_dict[i] = count
        for i in  list(df['variable_label']):
            df.loc[df['variable_label'] == i, ['total']] = self.raw_data[i].sum()
        df['asked_total'] = df['variable'].apply(lambda x: count_dict[x])

    def variable_value(self, variable, value):
        var_val = []
        for idx, var in enumerate(list(variable)):
            var_val.append(var+'__' + str(list(value)[idx]))
        return var_val
    
    
    def generate_variable_map(self, df, survey):
        asked_total_dict = df[['variable', 'asked_total']].set_index('variable').to_dict()['asked_total']
        for variable in self.variable_map:
            try:
                asked_total = asked_total_dict[variable]
            except:
                asked_total = 0
                self.variable_map.loc[self.variable_map['variable'] == variable, ['asked_total']] = asked_total
        self.variable_map.to_excel('./data/generated_'+survey+'_variable_map.xlsx')