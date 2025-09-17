#import modin.pandas as pd
import pandas as pd
import datetime
import numpy as np

class ToOutputFormat : 

    def __init__(self) : 
        pass

    
    def data_type_of_value(self, value) : 
        type_of_data = type(value)
        if type_of_data == pd.Series: 
            return "series"
        elif type_of_data == pd.DataFrame :
            return "dataframe"
        else : 
            return "single_value"

    def if_natural_or_index(self, value) : 
        idx = value.index 
        if self.if_multi_index(value)  : 
            return "having_index"
        else :
            return "natural_index"

    def if_multi_index(self, value) : 
        midx = value.index 
        if type(midx.to_list()[0]) == tuple : 
            return "multi_index"
        elif type(midx.to_list()[0]) != tuple and midx.name != None:
            return "simple_index"
        else : 
            return None

    def reconvert_dates(self, df):
        ''' This function will convert datetime 'Month_Unique' into original "Jan 2023", "Aug 2023" format.'''
        if isinstance(df, pd.Series):
            if df.name == "Month_Unique" and hasattr(df, 'dt'):
                df = df.dt.strftime("%b %Y")
                print("Converted Month_Unique Series to original format.")
            return df
        df = df.copy()
        if "Month_Unique" in df.columns:
            if hasattr(df['Month_Unique'], 'dt'):
                df['Month_Unique'] = df['Month_Unique'].dt.strftime("%b %Y")
                print("Converted Month_Unique column to original format.")
            return df
        else:
            print("No Month_Unique column found to convert.")
            return df

    def num_format(self, df):
        
        ''' Format numeric values to 4 decimal places '''
        # Handle Series objects
        if isinstance(df, pd.Series):
            # Only skip formatting if the Series name is "Year" (case-insensitive)
            if pd.api.types.is_numeric_dtype(df):
                if not (df.name and isinstance(df.name, str) and df.name.lower() == "year"):
                    df = df.apply(lambda x: f"{x:,.1f}" if pd.notnull(x) else "")
            return df
        # Handle DataFrame objects
        df = df.copy()
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        # Exclude the "Year" column if present (case-insensitive)
        numeric_cols = [col for col in numeric_cols if not (isinstance(col, str) and col.lower() == "year")]
        for col in numeric_cols:
            df[col] = df[col].apply(lambda x: f"{x:,.1f}" if pd.notnull(x) else "")
        return df

    def scale_millions(self, df):
        """
        If single value < 1,000,000 -> return as is
        If single value >= 1,000,000 -> divide by 1,000,000

        If Series/DataFrame column min < 1,000,000 -> no change
        If Series/DataFrame column min >= 1,000,000 -> divide entire column by 1,000,000
        """
        if isinstance(df, (int, float, np.number)):  # Single number
            return df if df < 1_000_000 else df / 1_000_000
        elif isinstance(df, pd.Series):  # Pandas Series
            if df.min() >= 1_000_000:
                scaled = df / 1_000_000
                if df.name:  # Only rename if Series has a name
                    scaled.name = f"{df.name}_in_Mn"
                return scaled
            return df
        elif isinstance(df, pd.DataFrame):  # DataFrame
            df = df.copy()
            new_cols = []
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    if df[col].min() >= 1_000_000:
                        df[col] = df[col] / 1_000_000
                        col = f"{col}_in_Mn"  # Append suffix if scaled
                new_cols.append(col)
            df.columns = new_cols
            return df
        else:
            raise TypeError("Input must be a number, pandas Series, or pandas DataFrame.")

    def rename_unnamed_to_value(self, df):
        """
        Renames any unnamed columns or Series to 'Value', 'Value1', 'Value2', etc.
        - For Series: if name is None, 0, or '', set to 'Value'
        - For DataFrame: for any column with name None, 0, or '', set to 'Value', 'Value1', ...
        """
        if isinstance(df, pd.Series):
            if not df.name or df.name == 0 or df.name == "":
                print("Renaming Series to 'Value'")
                df.name = "Value"
            return df
        elif isinstance(df, pd.DataFrame):
            new_cols = []
            value_count = 1
            for col in df.columns:
                if not col or col == 0 or col == "":
                    print(f"Renaming column '{col}' to 'Value{value_count}'")
                    name = "Value" if value_count == 1 else f"Value{value_count}"
                    new_cols.append(name)
                    value_count += 1
                else:
                    new_cols.append(col)
            df.columns = new_cols
            print("Renamed DataFrame columns:", df.columns)
            return df
        else:
            print("Input is neither a Series nor a DataFrame. No renaming applied.")
            return df

    def make_table_api(self,dataframe, index_type) :
        
        # Convert Series to DataFrame for table processing
        if isinstance(dataframe, pd.Series):
            dataframe = dataframe.to_frame()
            print('Converted Series to DataFrame for table processing.')
        # dataframe = self.rename_unnamed_to_value(dataframe)
        # print("dataframe renamed:", dataframe.columns)
        dataframe = dataframe.copy()
        found_month_in_index = False
        original_index_names = None
        
        if hasattr(dataframe.index, 'names') and dataframe.index.names:
            if 'Month_Unique' in dataframe.index.names:
                found_month_in_index = True
                original_index_names = dataframe.index.names.copy()
                print("Found Month_Unique in index names:")
                # dataframe = dataframe.reset_index()
                # dataframe = self.rename_unnamed_to_value(dataframe)
                # print("dataframe renamed after Month_Unique reset:", dataframe.columns)

        # --- Handle simple index ---
        if index_type == "simple_index":
            dataframe = dataframe.reset_index()
            print("Reset index for simple index type with columns:", dataframe.columns)
    
            # dataframe = self.rename_unnamed_to_value(dataframe)

            print("dataframe renamed after new columns :", dataframe.columns)
        
        # Apply date conversion (now Month_Unique is a column if it existed in index)
        print("Reconvert dates in dataframe.")
        # dataframe = self.reconvert_dates(df=dataframe)
        print("Successful dataframe after reconvert dates:", dataframe.columns)
        
        # If Month_Unique was originally in index, set it back as original index names
        if found_month_in_index and original_index_names and all(name in dataframe.columns for name in original_index_names):
            
            dataframe = dataframe.set_index(original_index_names) 
            print("simple index dataframe set back to original index names:", dataframe.index.names)
         
        if index_type == "multi_index" :
            dataframe = dataframe.reset_index()
            print("Reset index for multi index type with columns:", dataframe.columns)
            # dataframe = self.rename_unnamed_to_value(dataframe)
            print("dataframe renamed after new columns :", dataframe.columns)
        print("Dataframe", dataframe)
        dataframe = self.scale_millions(df = dataframe)
        dataframe = self.num_format(df = dataframe)
        print("Number formatted")
        print(dataframe)
        dataframe = dataframe.to_dict('records')
        dataframe_new = []
        for i, data in enumerate(dataframe) : 
            dataframe_new.append({**{"Sr.No.":i+1}, **data})
        return dataframe_new

    def make_chart_api(self, dataframe, index_type) : 
        # Convert Series to DataFrame for chart processing
        if isinstance(dataframe, pd.Series):
            dataframe = dataframe.to_frame()
        dataframe = self.rename_unnamed_to_value(dataframe)
        dataframe = dataframe.copy()
        found_month_in_index = False
        original_index_names = None
        
        if hasattr(dataframe.index, 'names') and dataframe.index.names:
            if 'Month_Unique' in dataframe.index.names:
                found_month_in_index = True
                original_index_names = dataframe.index.names.copy()
                dataframe = dataframe.reset_index()
                dataframe = self.rename_unnamed_to_value(dataframe)
        # Apply date conversion (now Month_Unique is a column if it existed in index)
        dataframe = self.reconvert_dates(df=dataframe)
        # If Month_Unique was originally in index, set it back as original index names
        if found_month_in_index and original_index_names and all(name in dataframe.columns for name in original_index_names):
            dataframe = dataframe.set_index(original_index_names)
        # dataframe = self.num_format(df = dataframe)
        dataframe = self.scale_millions(df = dataframe)

        if index_type == "multi_index" :
            index_data = dataframe.index
            xaxis = []
            for indx in index_data : 
                indx = [str(item) for item in indx]
                xaxis.append("_".join(indx))
            xaxis_name = "_and_".join(index_data.names)
            chart_type = 'bar_chart'
            yaxis = dataframe.iloc[:, 0].values.tolist()  # Get first column values
            yaxis_name = dataframe.columns[0]

        elif index_type == "simple_index" : 
            index_data = dataframe.index
            xaxis = index_data.tolist()
            xaxis_name = index_data.name if index_data.name else "Index"
            chart_type = 'bar_chart'
            yaxis = dataframe.iloc[:, 0].values.tolist()  # Get first column values
            yaxis_name = dataframe.columns[0]

        elif index_type == "natural_index" :
            non_number_column = dataframe.select_dtypes(exclude=['number']).columns.tolist()
            number_columns = dataframe.select_dtypes(include=['number']).columns.tolist()
            total_columns = dataframe.columns.tolist()
            if len(total_columns) == 1:
                # Handle single column case (converted Series)
                xaxis = list(range(len(dataframe)))
                xaxis_name = "Index"
                chart_type = 'bar_chart'
                yaxis = dataframe.iloc[:, 0].values.tolist()
                yaxis_name = dataframe.columns[0]
            elif len(total_columns) == 2 : 
                if len(non_number_column) == 1 and len(number_columns) == 1 : 
                    print("I am in total columns")
                    xaxis = dataframe.loc[:, non_number_column
                                            ].iloc[:,0].values.tolist()
                    xaxis_name = non_number_column[0]
                    chart_type = 'bar_chart'
                    yaxis = dataframe.loc[:, number_columns 
                                            ].iloc[:,0].values.tolist()
                    yaxis_name = number_columns[0]
                elif len(number_columns) == 2 : 
                    int_columns = dataframe.select_dtypes(include=['int64', 
                                                                'int32', 
                                                                'int8',
                                                                'int16']).columns.tolist()
                    if len(int_columns) == 1 : 
                        xaxis = dataframe.loc[:, int_columns
                                            ].iloc[:,0].values.tolist()
                        xaxis_name = int_columns[0]
                        chart_type = 'line_chart'
                        total_columns_temp = dataframe.columns.tolist()
                        total_columns_temp.remove(xaxis_name)
                        yaxis = dataframe.loc[:, 
                                            total_columns_temp 
                                            ].iloc[:,0].values.tolist()
                        yaxis_name = total_columns_temp[0] 
                    else : 
                        xaxis = dataframe.loc[:, [total_columns[0]]
                                            ].iloc[:,0].values.tolist()
                        xaxis_name = total_columns[0]
                        chart_type = 'line_chart'
                        total_columns_temp = dataframe.columns.tolist()
                        total_columns_temp.remove(xaxis_name)
                        yaxis = dataframe.loc[:, total_columns_temp 
                                            ].iloc[:,0].values.tolist()
                        yaxis_name = total_columns_temp[0] 
        if isinstance(yaxis, list):
            yaxis = [round(float(y), 1) if isinstance(y, (int, float)) else y for y in yaxis]
        return {
                    "xaxis" : xaxis,
                    "xaxis_name" : xaxis_name,
                    "yaxis" : yaxis,
                    "yaxis_name" : yaxis_name,
                    "chart_type" : chart_type
                }
        
    def return_simple_data(self, value) : 
        
        if self.data_type_of_value(value) == "single_value" : 
            if isinstance(value, (int, float)) and pd.notnull(value):
                if value < 1_000_000:
                    return f"{value:,.1f}"
                if value >= 1_000_000:
                    value = self.scale_millions(value)
                    return f"{value:,.1f}"
            else:
                return str(value)
                
        elif self.if_natural_or_index(value)  == "natural_index" : 
            value = value.copy()
            value = self.rename_unnamed_to_value(value)
            value = self.reconvert_dates(df = value)  # Apply date conversion first
            value = self.scale_millions(df = value)   # Scale millions
            value = self.num_format(df = value)       # Apply number formatting
            value.index = value.index + 1             # Then modify index
            value_str = str(value.to_markdown()) 
            print(value_str)    
            return value_str
            
        else : 
            value = value.reset_index()
            value = self.rename_unnamed_to_value(value)
            value = self.reconvert_dates(df = value)
            value = self.scale_millions(df = value)
            value = self.num_format(df = value)
            value.index = value.index + 1 
            value_str = str(value.to_markdown()) 
            return value_str

    def return_tabular_data(self, value) : 
        if self.data_type_of_value(value) == "single_value" : 
            if isinstance(value, (int, float)) and pd.notnull(value):
                if value < 1_000_000:
                    return f"{value:,.1f}"
                if value >= 1_000_000:
                    value = self.scale_millions(value)
                    return f"{value:,.1f}"
            else:
                return str(value)
        elif self.if_natural_or_index(value)  == "natural_index" : 
            value = self.rename_unnamed_to_value(value)
            value = self.reconvert_dates(df = value)
            return self.make_table_api(value, index_type = "natural_index")
        else : 
            value = value.reset_index()
            value = self.rename_unnamed_to_value(value)
            value = self.reconvert_dates(df = value)
            return self.make_table_api(value, index_type = "natural_index")

    def return_chart_data(self, value) : 
        if self.data_type_of_value(value) == "single_value" : 
            return None
        elif self.if_multi_index(value)  == "multi_index" : 
            return self.make_chart_api(dataframe = value,
                                      index_type = "multi_index")
        elif self.if_multi_index(value)  == "simple_index" : 
            return self.make_chart_api(dataframe = value,
                                      index_type = "simple_index")
        else : 
            return self.make_chart_api(dataframe = value,
                                      index_type = "natural_index")
        
    def api_output(self, value, user_email=None, query_type=None, questions=None):
        
        output = {
        "answer": {
            "code": 1,
            "response": {
                "query": value,
                "output": value
                }
        },
        "userEmail": user_email,
        "queryType": query_type,
        "questions": questions,
        "prompt": {},
        "mapping": ['map fields']}
        return output

    def return_data(self, value, output_type, user_email, questions):

        print("1. in output format return data output", value["output"])
        print("2. in output format return data query", value["query"])
        
        if output_type == "simple":

            
            out_data = [{'query':query ,'output': self.return_simple_data(out) }  
                        for query, out in zip(value['query'] ,value['output'])] 
            
            print("1. in output format return data simple out data", str(out_data))

            output = {
                        "answer": {
                            "code": 1,
                            "response": out_data
                        },
                        "userEmail": user_email,
                        "queryType": "simple",
                        "questions": questions,
                        "prompt": {},
                        "mapping": ['map fields']
                    } 
            
            return output

        
        elif output_type == "table" :

            for i in range(0, len(value['output'])) :

                my_val = value['output'][i]

                print("5. in output format return data my_val out ", str(my_val))
                
                if type(my_val) != pd.Series  and type(my_val) != pd.DataFrame : 

                    value['output'][i] = pd.DataFrame({"Output_Value":[my_val]})

            out_data = [{'query':query ,'output': self.return_tabular_data(out) }  
                        for query, out in zip(value['query'] ,value['output'])] 
            
            print("2. in output format return data table out data", str(out_data))
            
            output = {
                        "answer": {
                            "code": 1,
                            "response": out_data
                        },
                        "userEmail": user_email,
                        "queryType": "table",
                        "questions": questions,
                        "prompt": {},
                        "mapping": ['map fields']
                    } 
            return output 
            
        elif output_type == "graph" : 

            for i in range(0, len(value['output'])) :

                my_val = value['output'][i]

                print("6. in output format return data my_val out ", str(my_val))

                if type(my_val) != pd.Series  and type(my_val) != pd.DataFrame : 
                    value['output'][i] = pd.DataFrame({"Output_Value":[my_val]})

            out_data = [{'query':query ,'output': self.return_chart_data(out) }  
                        for query, out in zip(value['query'] ,value['output'])] 
            
            print("3. in output format return data graph out data", str(out_data))

            output = {
                        "answer": {
                            "code": 1,
                            "response": out_data
                        },
                        "userEmail": user_email,
                        "queryType": "graph",
                        "questions": questions,
                        "prompt": {},
                        "mapping": ['map fields']
                    } 
            return output