import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None

class Bivariate():
    """
        Prepares data for Bivariate analysis
        ----------------------------

        Input: 
            raw data: raw data of C2M2 Kathmandu survey in xlsx format (either business survey or workforce survey)
            variable map: mapping of variable informations (e.g. input type, total responses etc.) in xlsx format
            labels map: mapping of label and variables information in xlsx format
        
        ----------------------------
    """

    def __init__(self, raw_data, variable_map, labels_map):
        "Initialiaze Bivariate class with required parameters"
        self.raw_data = raw_data
        self.variable_map = variable_map
        self.labels_map = labels_map
        self.selected_variables = self.variable_map[self.variable_map['selected'] ==1] # Filter variables so that only selected ones are used foe bivariate analysis
        self.labels_map['variableLabel'] = self.variable_value(self.labels_map['variable'], self.labels_map['value']) 
        self.groupby_columns = self.selected_variables[self.selected_variables['inputType'] == 'groupby']['variable'].values.tolist()
        self.s_select_variables = self.selected_variables[self.selected_variables['inputType'] == 'single-select']['variable'].values.tolist()
        self.m_select_variables = self.selected_variables[self.selected_variables['inputType'] == 'multi-select']['variable'].values.tolist()


    def generate_bivariate(self):
        bivariate_stats = pd.concat([self.generate_singleselect(),self.generate_multiselect()])
        bivariate_stats['index'] = list(range(1, len(bivariate_stats)+1))
        queue_dict = self.variable_map[['variable', 'queueIndex']].set_index('variable').to_dict()['queueIndex']
        bivariate_stats['queueIndex'] = bivariate_stats['yVariable'].apply(lambda x: queue_dict[x])
        weight = 0.1**len(str(len(bivariate_stats)))
        bivariate_stats['queueId'] = bivariate_stats['queueIndex'] +  weight* bivariate_stats['index']
        bivariate_stats = bivariate_stats.sort_values(by='queueId')
        bivariate_stats['index'] = list(range(1, len(bivariate_stats)+1))
        bivariate_stats.drop(['queueId', 'queueIndex', 'labelIndex', 'askedTotal'], axis=1, inplace=True)
        return bivariate_stats
        
    def generate_singleselect(self):
        s_select_bivariate_variables = self.s_select_variables + self.groupby_columns
        s_select_raw = self.raw_data[s_select_bivariate_variables]
        s_select_bivariate = []
        for x_variable in self.groupby_columns:
            for y_variable in self.s_select_variables:
                xy_tuple = self.create_xy_tuple(x_variable, y_variable)
                x_label_dict, y_label_dict = self.create_label_dict(x_variable, y_variable, select_type='singleselect')
                s_select_each = self.generate_total(s_select_raw, x_variable, y_variable, select_type='singleselect', xy_tuple=xy_tuple)
                s_select_each = self.add_labels(s_select_each, x_label_dict, y_label_dict, x_variable, y_variable, select_type='singleselect')
                s_select_each.sort_values('labelIndex', inplace=True, ascending=False)
                if len(s_select_bivariate)==0:
                    s_select_bivariate = s_select_each
                else:
                    s_select_bivariate = pd.concat((s_select_bivariate, s_select_each))
        return s_select_bivariate

    

    def generate_multiselect(self):
        m_select_variable_label = self.labels_map.set_index('variable').transpose()[self.m_select_variables].transpose()['variableLabel'].values.tolist()
        m_select_bivariate_labels = m_select_variable_label + self.groupby_columns
        m_select_raw = self.raw_data[m_select_bivariate_labels]
        m_select_bivariate = []
        for x_variable in self.groupby_columns:
            for y_variable_list in self.m_select_variables:
                label_df = self.labels_map[self.labels_map['variable']==y_variable_list][['variableLabel', 'label_en','labelIndex']].sort_values('labelIndex')
                variable_df = []
                for y_variable in label_df['variableLabel']:
                    x_label_dict, y_label_dict = self.create_label_dict(x_variable, y_variable, select_type='multiselect')
                    m_select_each_df = self.generate_total(m_select_raw, x_variable, y_variable, select_type='multiselect')
                    m_select_each_df = self.add_labels(m_select_each_df, x_label_dict, y_label_dict, x_variable, y_variable, select_type='multiselect')
                    if len(variable_df)==0:
                        variable_df = m_select_each_df
                    else:
                        variable_df = pd.concat((variable_df, m_select_each_df))
                variable_df.sort_values('labelIndex', inplace=True, ascending=False)
                if len(m_select_bivariate)==0:
                    m_select_bivariate = variable_df
                else:
                    m_select_bivariate = pd.concat((m_select_bivariate, variable_df))
        return m_select_bivariate
    
    
    def create_label_dict(self, x_variable, y_variable, select_type):
        if select_type=='multiselect':
            y_variable = y_variable.split('__')[0]
        labels_list = ['value', 'label_en', 'label_ne', 'labelIndex']
        x_label_dict = self.labels_map.loc[self.labels_map['variable']==x_variable][labels_list].sort_values('labelIndex').set_index('value').to_dict()
        y_label_dict = self.labels_map.loc[self.labels_map['variable']==y_variable][labels_list].sort_values('labelIndex').set_index('value').to_dict()
        return x_label_dict, y_label_dict
    
    
    def generate_total(self, selected_raw_data, x_variable, y_variable, select_type, xy_tuple=None):
        label_queue = self.labels_map[['variableLabel', 'labelIndex']].set_index('variableLabel').to_dict()['labelIndex']
        if select_type=='multiselect':
            each_df = pd.DataFrame(selected_raw_data.groupby(x_variable)[y_variable].sum())
            each_df.columns = ['total']
            each_df.reset_index(inplace=True)
            each_df['yValue'] = int(y_variable.split('__')[1])
            each_df['yVariable'] = y_variable.split('__')[0]
            each_df['xVariable'] = x_variable
            each_df.columns = ['xValue', 'total','yValue', 'yVariable', 'xVariable']
        else:
            each_df = pd.DataFrame(selected_raw_data.groupby([x_variable])[y_variable].value_counts())
            each_df.columns = ['total']
            each_df = each_df.to_dict()
            for i in xy_tuple:
                if i not in each_df['total'].keys():
                    each_df['total'][i] = 0
            each_df = pd.DataFrame(each_df)
            each_df['yVariable'] = y_variable
            each_df.reset_index(inplace=True)
            each_df['xVariable'] = x_variable
            each_df.columns = ['xValue', 'yValue', 'total', 'yVariable', 'xVariable']
        each_df['xVariableLabel'] = self.variable_value(each_df['xVariable'], each_df['xValue'])
        each_df['yVariableLabel'] = self.variable_value(each_df['yVariable'], each_df['yValue'])
        each_df['xLabelQueue'] = each_df['xVariableLabel'].apply(lambda x: label_queue[x])
        each_df['yLabelQueue'] = each_df['yVariableLabel'].apply(lambda x: label_queue[x])
        each_df['labelIndex'] =  each_df['xLabelQueue'] + 0.001 *  each_df['yLabelQueue']
        return each_df
    
    
    def add_labels(self, df, x_label_dict, y_label_dict, x_variable, y_variable, select_type):
        # print()
        df['yLabel_en'] = df['yValue'].apply(lambda x: y_label_dict['label_en'][x])
        df['yLabel_ne'] = df['yValue'].apply(lambda x: y_label_dict['label_ne'][x])
        df['xLabel_en'] = df['xValue'].apply(lambda x: x_label_dict['label_en'][x])
        df['xLabel_ne'] = df['xValue'].apply(lambda x: x_label_dict['label_ne'][x])
        asked_total_dict = self.variable_map[['variable', 'askedTotal']].set_index('variable').to_dict()['askedTotal']
        df['askedTotal'] = df['yVariable'].apply(func=lambda x: asked_total_dict[x])
        df['percOfTotal'] = df['total'] / df['askedTotal']
        df['variableGroup'] = self.variable_map[self.variable_map['variable']==y_variable.split('__')[0]]['group'].tolist()[0]
        df = df[['xVariable', 'xValue', 'xLabel_en','xLabel_ne', 'yVariable', 'yValue','yLabel_en','yLabel_ne', 'total','percOfTotal','askedTotal', 'variableGroup', 'labelIndex']]
        return df
    
    
    def create_xy_tuple(self, x_variable, y_variable):
        xy_tuple = []
        for i in self.labels_map[self.labels_map['variable'] == x_variable].value:
            for j in self.labels_map[self.labels_map['variable'] == y_variable].value:
                xy_tuple.append((i,j))
        return xy_tuple
    
    
    def variable_value(self, variable, value):
        var_val = []
        for idx, var in enumerate(list(variable)):
            var_val.append(var+'__' + str(int(list(value)[idx])))
        return var_val