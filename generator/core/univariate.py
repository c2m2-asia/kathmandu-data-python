import numpy as np
import pandas as pd

class Univariate():
    def __init__(self, raw_data, variable_map, labels_map):
        self.raw_data = raw_data
        self.variable_map = variable_map[['variable', 'ques__ne', 'ques__en', 'asked_total', 'queue_index', 'selected', 'input_type', 'group', 'asked_condition']]
        self.labels_map = labels_map
        self.selected_variables = self.variable_map[self.variable_map['selected'] ==1]
        self.s_select_variables = self.selected_variables[self.selected_variables['input_type'] == 'single-select']['variable'].values.tolist()
        self.groupby_columns = self.selected_variables[self.selected_variables['input_type'] == 'groupby']['variable'].values.tolist()
        self.s_select_variables = self.s_select_variables + self.groupby_columns
        self.m_select_variables = self.selected_variables[self.selected_variables['input_type'] == 'multi-select']['variable'].values.tolist()
    
    def generate_univariate(self):
        univariate_stats = pd.concat([self.generate_singleselect(),self.generate_multiselect()])
        self.generate_label_map(univariate_stats)
        univariate_stats.drop('variable_label', axis=1, inplace=True)
        univariate_stats['index'] = list(range(1, len(univariate_stats)+1))
        queue_dict = self.variable_map[['variable', 'queue_index']].set_index('variable').to_dict()['queue_index']
        univariate_stats['queue_index'] = univariate_stats['variable'].apply(lambda x: queue_dict[x])
        weight = 0.1**len(str(len(univariate_stats)))
        univariate_stats['queue_id'] = univariate_stats['queue_index'] +  weight* univariate_stats['index']
        univariate_stats = univariate_stats.sort_values(by='queue_id')
        univariate_stats['index'] = list(range(1, len(univariate_stats)+1))
        univariate_stats.drop(['queue_id', 'queue_index'], axis=1, inplace=True)
        return univariate_stats
    
    
    def generate_singleselect(self):
        self.initiallize_columns()
        s_select_map = self.labels_map.set_index('variable').transpose()[self.s_select_variables]
        s_select_map = s_select_map.transpose().reset_index()
        s_select_univariate = self.s_select_generate_total(s_select_map)
        s_select_univariate['perc_of_total'] = s_select_univariate['total'] / s_select_univariate['asked_total']
        s_select_univariate['universe'] = len(self.raw_data)
        s_select_univariate['total'] = s_select_univariate['total'].astype(int)
        s_select_univariate['perc_of_total'].fillna(0, inplace=True)
        s_select_univariate = s_select_univariate[['variable', 'value', 'label__en', 'label__ne', 'variable_group', 'total', 'perc_of_total', 'asked_total', 'universe', 'variable_label', 'label_index']]
        return s_select_univariate
    
    def generate_multiselect(self):
        self.initiallize_columns()
        m_select_map = self.labels_map.set_index('variable').transpose()[self.m_select_variables]
        m_select_map = m_select_map.transpose().reset_index()
        m_select_univariate = self.m_select_generate_total(m_select_map)
        m_select_univariate['universe'] = len(self.raw_data)
        m_select_univariate['perc_of_total'] = m_select_univariate['total'] / m_select_univariate['asked_total']
        m_select_univariate['total'] = m_select_univariate['total'].astype(int)
        m_select_univariate['perc_of_total'].fillna(0, inplace=True)
        m_select_univariate = m_select_univariate[['variable', 'value', 'label__en', 'label__ne', 'variable_group', 'total', 'perc_of_total', 'asked_total', 'universe', 'variable_label', 'label_index']]
        return m_select_univariate
    
    def initiallize_columns(self):
        self.labels_map[['total', 'perc_of_total', 'asked_total']] = 0
        self.labels_map['universe'] = self.raw_data.shape[0]
        self.labels_map['label_index']=None
        self.labels_map['variable_label'] = self.variable_value(self.labels_map['variable'], self.labels_map['value'])
        
    def s_select_generate_total(self, df):
        label_dict = self.labels_map.set_index('variable_label')[['label__en', 'label__ne', 'variable_group', 'universe']].transpose().to_dict()
        group_dict = self.variable_map.set_index('variable')[['group']].transpose().to_dict()
        s_select_univariate = []
        variables = df['variable'].unique()
        for i in variables:
            variable_df = pd.DataFrame(self.raw_data[i].value_counts()).reset_index()
            variable_df = variable_df.astype(int)
            variable_df['variable_label'] = variable_df['index'].apply(lambda x: i+'__'+ str(x))
            variable_df.columns = ['value', 'total', 'variable_label']
            variable_df['variable'] = i
            variable_df['asked_total'] = variable_df['total'].sum()
            variable_df['label__en'] = variable_df['variable_label'].apply( lambda x: label_dict[x]['label__en'])
            variable_df['label__ne'] = variable_df['variable_label'].apply( lambda x: label_dict[x]['label__ne'])
            variable_df['variable_group'] = variable_df['variable'].apply( lambda x: group_dict[x]['group'])
            variable_df.sort_values('total', inplace=True)
            variable_df['label_index'] = list(range(2, len(variable_df)+2))
            variable_df = variable_df[['variable', 'value', 'label__en', 'label__ne', 'variable_group', 'total', 'asked_total', 'variable_label', 'label_index']]
            if len(s_select_univariate)==0:
                s_select_univariate = variable_df
            else:
                s_select_univariate = pd.concat((s_select_univariate, variable_df))
            
        return s_select_univariate

                
    def m_select_generate_total(self, df):
        label_dict = self.labels_map.set_index('variable_label')[['label__en', 'label__ne', 'variable_group', 'universe']].transpose().to_dict()
        group_dict = self.variable_map.set_index('variable')[['group']].transpose().to_dict()
        variable_label = {}
        for i in df['variable']:
            variable_label[i] = list(df[df['variable']==i]['variable_label'])
        count_dict = {}
        for i in variable_label.keys():
            count = 0
            for j in self.raw_data[variable_label[i]].values:
                if (1 in j) or (0 in j):
                    count +=1
            count_dict[i] = count
        m_select_univariate = []
        for i in variable_label.keys():
            variable_df = pd.DataFrame(self.raw_data[variable_label[i]])
            variable_df = pd.DataFrame(variable_df.sum()).reset_index()
            variable_df.columns = ['variable_label', 'total']
            variable_df.sort_values('total', inplace=True)
            variable_df['label_index'] = list(range(2, len(variable_df)+2))
            variable_df['variable'] = i
            variable_df['value'] = variable_df['variable_label'].apply(lambda x: x.split('__')[1])
            variable_df['asked_total'] = count_dict[i]
            variable_df['label__en'] = variable_df['variable_label'].apply( lambda x: label_dict[x]['label__en'])
            variable_df['label__ne'] = variable_df['variable_label'].apply( lambda x: label_dict[x]['label__ne'])
            variable_df['variable_group'] = variable_df['variable'].apply( lambda x: group_dict[x]['group'])
            variable_df.loc[variable_df['label__en']=='Other', 'label_index'] = 1
            variable_df.loc[variable_df['label__en']=='other', 'label_index'] = 1
            variable_df.loc[variable_df['label__en']=='Others', 'label_index'] = 1
            variable_df.loc[variable_df['label__en']=='None of the above', 'label_index'] = 0
            variable_df.sort_values('label_index', inplace=True)
            if len(m_select_univariate)==0:
                m_select_univariate = variable_df
            else:
                m_select_univariate = pd.concat((m_select_univariate, variable_df))
        return m_select_univariate

    def variable_value(self, variable, value):
        var_val = []
        for idx, var in enumerate(list(variable)):
            var_val.append(var+'__' + str(list(value)[idx]))
        return var_val
    
    
    def generate_variable_map(self, df, survey):
        asked_total_dict = df[['variable', 'asked_total']].set_index('variable').to_dict()['asked_total']
        for variable in self.variable_map['variable']:
            try:
                asked_total = asked_total_dict[variable]
            except:
                asked_total = 0
            self.variable_map.loc[self.variable_map['variable'] == variable, ['asked_total']] = asked_total
        self.variable_map['universe'] = self.raw_data.shape[0]
        self.variable_map['asked_total'].fillna(self.raw_data.shape[0], inplace=True)
        self.variable_map['asked_condition'].fillna('general', inplace=True)
        self.variable_map['queue_index'].fillna(0, inplace=True)
        final_variable_map = self.variable_map[self.variable_map['selected']==1]
        final_variable_map.sort_values('queue_index', inplace=True)
        final_variable_map.to_excel('./data/generated_'+survey+'_variable_map.xlsx', index=False)

    def generate_label_map(self, df):
        label_queue = df[['variable_label', 'label_index']].set_index('variable_label').to_dict()['label_index']
        for i in df['variable_label']:
            self.labels_map.loc[self.labels_map['variable_label']==i, 'label_index'] = label_queue[i]
        