import pandas as pd

class Bivariate():
    def __init__(self, raw_data, variable_map, labels_map):
        self.raw_data = raw_data
        self.variable_map = variable_map
        self.labels_map = labels_map
        self.selected_variables = self.variable_map[self.variable_map['selected'] ==1]
        self.labels_map['variable_label'] = self.variable_value(self.labels_map['variable'], self.labels_map['value'])
        self.groupby_columns = self.selected_variables[self.selected_variables['input_type'] == 'groupby']['variable'].values.tolist()
        self.s_select_variables = self.selected_variables[self.selected_variables['input_type'] == 'single-select']['variable'].values.tolist()
        self.m_select_variables = self.selected_variables[self.selected_variables['input_type'] == 'multi-select']['variable'].values.tolist()
        
        
    def generate_bivariate(self):
        bivariate_stats = pd.concat([self.generate_singleselect(),self.generate_multiselect()])
        bivariate_stats['index'] = list(range(1, len(bivariate_stats)+1))
        queue_dict = self.variable_map[['variable', 'queue_index']].set_index('variable').to_dict()['queue_index']
        bivariate_stats['queue_index'] = bivariate_stats['y_variable'].apply(lambda x: queue_dict[x])
        weight = 0.1**len(str(len(bivariate_stats)))
        bivariate_stats['queue_id'] = bivariate_stats['queue_index'] +  weight* bivariate_stats['index']
        bivariate_stats = bivariate_stats.sort_values(by='queue_id')
        bivariate_stats['index'] = list(range(1, len(bivariate_stats)+1))
        bivariate_stats.drop(['queue_id', 'queue_index'], axis=1, inplace=True)
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
                if len(s_select_bivariate)==0:
                    s_select_bivariate = s_select_each
                else:
                    s_select_bivariate = pd.concat((s_select_bivariate, s_select_each))
        return s_select_bivariate

    

    def generate_multiselect(self):
        m_select_variable_label = self.labels_map.set_index('variable').transpose()[self.m_select_variables].transpose()['variable_label'].values.tolist()
        m_select_bivariate_labels = m_select_variable_label + self.groupby_columns
        m_select_raw = self.raw_data[m_select_bivariate_labels]
        m_select_bivariate = []
        for x_variable in self.groupby_columns:
            for y_variable in m_select_variable_label:
                x_label_dict, y_label_dict = self.create_label_dict(x_variable, y_variable, select_type='multiselect')
                m_select_each_df = self.generate_total(m_select_raw, x_variable, y_variable, select_type='multiselect')
                m_select_each_df = self.add_labels(m_select_each_df, x_label_dict, y_label_dict, x_variable, y_variable, select_type='multiselect')
                if len(m_select_bivariate)==0:
                    m_select_bivariate = m_select_each_df
                else:
                    m_select_bivariate = pd.concat((m_select_bivariate, m_select_each_df))
        return m_select_bivariate
    
    
    def create_label_dict(self, x_variable, y_variable, select_type):
        if select_type=='multiselect':
            y_variable = y_variable.split('__')[0]
        labels_list = ['value', 'label_en', 'label_ne', 'variable_group']
        x_label_dict = self.labels_map.loc[ self.labels_map['variable']==x_variable][labels_list].set_index('value').to_dict()
        y_label_dict = self.labels_map.loc[ self.labels_map['variable']==y_variable][labels_list].set_index('value').to_dict()
        return x_label_dict, y_label_dict
    
    
    def generate_total(self, selected_raw_data, x_variable, y_variable, select_type, xy_tuple=None):
        if select_type=='multiselect':
            each_df = pd.DataFrame(selected_raw_data.groupby(x_variable)[y_variable].sum())
            each_df.columns = ['total']
        else:
            each_df = pd.DataFrame(selected_raw_data.groupby([x_variable])[y_variable].value_counts())
            each_df.columns = ['total']
            each_df = each_df.to_dict()
            for i in xy_tuple:
                if i not in each_df['total'].keys():
                    each_df['total'][i] = 0
            each_df = pd.DataFrame(each_df)
        return each_df
    
    
    def add_labels(self, df, x_label_dict, y_label_dict, x_variable, y_variable, select_type):
        df.reset_index(inplace=True)
        df['x_variable'] = x_variable
        if select_type=='multiselect':
            df['y_variable'] = y_variable.split('__')[0]
            df['y_value'] = int(y_variable.split('__')[1])
            df.columns = ['x_value', 'total', 'x_variable', 'y_variable', 'y_value']
        else:
            df['y_variable'] = y_variable
            df.columns = ['x_value', 'y_value', 'total', 'x_variable', 'y_variable']
        df['y_label_en'] = df['y_value'].apply(lambda x: y_label_dict['label_en'][x])
        df['y_label_ne'] = df['y_value'].apply(lambda x: y_label_dict['label_ne'][x])
        df['x_label_en'] = df['x_value'].apply(lambda x: x_label_dict['label_en'][x])
        df['x_label_ne'] = df['x_value'].apply(lambda x: x_label_dict['label_ne'][x])
        asked_total_dict = self.variable_map[['variable', 'asked_total']].set_index('variable').to_dict()['asked_total']
        df['asked_total'] = df['y_variable'].apply(func=lambda x: asked_total_dict[x])
        df['perc_of_total'] = df['total'] / df['asked_total']
        df['variable_group'] = y_label_dict['variable_group'][1]
        df = df[['x_variable', 'x_value', 'x_label_en','x_label_ne', 'y_variable', 'y_value','y_label_en','y_label_ne', 'total','perc_of_total','asked_total', 'variable_group']]
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
            var_val.append(var+'__' + str(list(value)[idx]))
        return var_val