import numpy as np
import pandas as pd

class Univariate():
    def __init__(self, raw_data, variable_map, labels_map):
        self.raw_data = raw_data
        self.variable_map = variable_map[['variable', 'ques_ne', 'ques_en', 'askedTotal', 'queueIndex', 'selected', 'inputType', 'group', 'askedCondition', 'subGroups', 'highlights']]
        self.labels_map = labels_map
        self.selected_variables = self.variable_map[self.variable_map['selected'] ==1]
        self.s_select_variables = self.selected_variables[self.selected_variables['inputType'] == 'single-select']['variable'].values.tolist()
        self.groupby_columns = self.selected_variables[self.selected_variables['inputType'] == 'groupby']['variable'].values.tolist()
        self.s_select_variables = self.s_select_variables + self.groupby_columns
        self.m_select_variables = self.selected_variables[self.selected_variables['inputType'] == 'multi-select']['variable'].values.tolist()
    
    def generate_univariate(self):
        univariate_stats = pd.concat([self.generate_singleselect(),self.generate_multiselect()])
        self.generate_label_map(univariate_stats)
        univariate_stats.drop('variableLabel', axis=1, inplace=True)
        univariate_stats['index'] = list(range(1, len(univariate_stats)+1))
        queue_dict = self.variable_map[['variable', 'queueIndex']].set_index('variable').to_dict()['queueIndex']
        univariate_stats['queueIndex'] = univariate_stats['variable'].apply(lambda x: queue_dict[x])
        weight = 0.1**len(str(len(univariate_stats)))
        univariate_stats['queueId'] = univariate_stats['queueIndex'] +  weight* univariate_stats['index']
        univariate_stats = univariate_stats.sort_values(by='queueId')
        univariate_stats['index'] = list(range(1, len(univariate_stats)+1))
        univariate_stats.drop(['queueId', 'queueIndex'], axis=1, inplace=True)
        return univariate_stats

    def generate_singleselect(self):
        self.initiallize_columns()
        s_select_map = self.labels_map.set_index('variable').transpose()[self.s_select_variables]
        s_select_map = s_select_map.transpose().reset_index()
        s_select_univariate = self.s_select_generate_total(s_select_map)
        s_select_univariate['percOfTotal'] = s_select_univariate['total'] / s_select_univariate['askedTotal']
        s_select_univariate['universe'] = len(self.raw_data)
        s_select_univariate['total'] = s_select_univariate['total'].astype(int)
        s_select_univariate['percOfTotal'].fillna(0, inplace=True)
        s_select_univariate = s_select_univariate[['variable', 'value', 'label_en', 'label_ne', 'variableGroup', 'total', 'percOfTotal', 'askedTotal', 'universe', 'variableLabel', 'labelIndex']]
        return s_select_univariate
    
    def generate_multiselect(self):
        self.initiallize_columns()
        m_select_map = self.labels_map.set_index('variable').transpose()[self.m_select_variables]
        m_select_map = m_select_map.transpose().reset_index()
        m_select_univariate = self.m_select_generate_total(m_select_map)
        m_select_univariate['universe'] = len(self.raw_data)
        m_select_univariate['percOfTotal'] = m_select_univariate['total'] / m_select_univariate['askedTotal']
        m_select_univariate['total'] = m_select_univariate['total'].astype(int)
        m_select_univariate['percOfTotal'].fillna(0, inplace=True)
        m_select_univariate = m_select_univariate[['variable', 'value', 'label_en', 'label_ne', 'variableGroup', 'total', 'percOfTotal', 'askedTotal', 'universe', 'variableLabel', 'labelIndex']]
        return m_select_univariate
    
    def initiallize_columns(self):
        self.labels_map[['total', 'percOfTotal', 'askedTotal']] = 0
        self.labels_map['universe'] = self.raw_data.shape[0]
        self.labels_map['labelIndex']=None
        self.labels_map['variableLabel'] = self.variable_value(self.labels_map['variable'], self.labels_map['value'])
        
    def s_select_generate_total(self, df):
        label_dict = self.labels_map.set_index('variableLabel')[['label_en', 'label_ne']].transpose().to_dict()
        group_dict = self.variable_map.set_index('variable')[['group']].transpose().to_dict()
        s_select_univariate = []
        variables = df['variable'].unique()
        for i in variables:
            variable_df = pd.DataFrame(self.raw_data[i].value_counts()).reset_index()
            variable_df = variable_df.astype(int)
            variable_df['variableLabel'] = variable_df['index'].apply(lambda x: i+'__'+ str(x))
            variable_df.columns = ['value', 'total', 'variableLabel']
            variable_df['variable'] = i
            variable_df['askedTotal'] = variable_df['total'].sum()
            variable_df['label_en'] = variable_df['variableLabel'].apply( lambda x: label_dict[x]['label_en'])
            variable_df['label_ne'] = variable_df['variableLabel'].apply( lambda x: label_dict[x]['label_ne'])
            variable_df['variableGroup'] = variable_df['variable'].apply( lambda x: group_dict[x]['group'])
            variable_df.sort_values('total', inplace=True)
            variable_df['labelIndex'] = list(range(2, len(variable_df)+2))
            variable_df = variable_df[['variable', 'value', 'label_en', 'label_ne', 'variableGroup', 'total', 'askedTotal', 'variableLabel', 'labelIndex']]
            if len(s_select_univariate)==0:
                s_select_univariate = variable_df
            else:
                s_select_univariate = pd.concat((s_select_univariate, variable_df))
            
        return s_select_univariate

                
    def m_select_generate_total(self, df):
        label_dict = self.labels_map.set_index('variableLabel')[['label_en', 'label_ne']].transpose().to_dict()
        group_dict = self.variable_map.set_index('variable')[['group']].transpose().to_dict()
        variable_label = {}
        for i in df['variable']:
            variable_label[i] = list(df[df['variable']==i]['variableLabel'])
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
            variable_df.columns = ['variableLabel', 'total']
            variable_df.sort_values('total', inplace=True)
            variable_df['labelIndex'] = list(range(2, len(variable_df)+2))
            variable_df['variable'] = i
            variable_df['value'] = variable_df['variableLabel'].apply(lambda x: x.split('__')[1])
            variable_df['askedTotal'] = count_dict[i]
            variable_df['label_en'] = variable_df['variableLabel'].apply( lambda x: label_dict[x]['label_en'])
            variable_df['label_ne'] = variable_df['variableLabel'].apply( lambda x: label_dict[x]['label_ne'])
            variable_df['variableGroup'] = variable_df['variable'].apply( lambda x: group_dict[x]['group'])
            variable_df.loc[variable_df['label_en']=='Other', 'labelIndex'] = 1
            variable_df.loc[variable_df['label_en']=='other', 'labelIndex'] = 1
            variable_df.loc[variable_df['label_en']=='Others', 'labelIndex'] = 1
            variable_df.loc[variable_df['label_en']=='None of the above', 'labelIndex'] = 0
            variable_df.sort_values('labelIndex', inplace=True)
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
        asked_total_dict = df[['variable', 'askedTotal']].set_index('variable').to_dict()['askedTotal']
        for variable in self.variable_map['variable']:
            try:
                asked_total = asked_total_dict[variable]
            except:
                asked_total = 0
            self.variable_map.loc[self.variable_map['variable'] == variable, ['askedTotal']] = asked_total
        self.variable_map['universe'] = self.raw_data.shape[0]
        self.variable_map['askedTotal'].fillna(self.raw_data.shape[0], inplace=True)
        self.variable_map['askedCondition'].fillna('general', inplace=True)
        self.variable_map['queueIndex'].fillna(0, inplace=True)
        final_variable_map = self.variable_map[self.variable_map['selected']==1]
        final_variable_map.sort_values('queueIndex', inplace=True)
        final_variable_map.to_excel('./data/generated_'+survey+'_variable_map.xlsx', index=False)

    def generate_label_map(self, df):
        label_queue = df[['variableLabel', 'labelIndex']].set_index('variableLabel').to_dict()['labelIndex']
        for i in df['variableLabel']:
            self.labels_map.loc[self.labels_map['variableLabel']==i, 'labelIndex'] = label_queue[i]
        