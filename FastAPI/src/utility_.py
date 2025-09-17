import pandas as pd
#import modin.pandas as pd
from pandas.api.types import CategoricalDtype

import time
from urllib.parse import quote_plus
from sqlalchemy import create_engine

from src.logger import LoggingTool
logger_tool = LoggingTool(filename="finbot_test", is_console=True)
logger = logger_tool.create_and_set_logger()

# Global vars
engine = None
load_progress = {}

#columns = ["Cargo_Type_Name","Cargo_Type_Details_Code","Commodity_Sub_Type_Code","Contract_Product_Lvl1","Contract_Product_Lvl2","Contract_Product_Segment","Contract_Length","Contractual_Customer_Code","Contractual_Customer_Name","Contractual_Customer_Country_Name","Contractual_Customer_Area_Name","Contractual_Customer_Region_Name","Contractual_Customer_Concern_Code","Contractual_Customer_Concern_Name","Contractual_Customer_Consolidated_Code","Contractual_Customer_Consolidated_Name","Contractual_Customer_Value_Proposition","Contractual_Customer_Vertical","Week","Month_Unique","Month_Code_YYYYMM","Quarter_Unique","Quarter_Code_YYYYQQ","Year","Equipment_SubType_Code","Equipment_Type_Code","Equipment_SubSize_Code","Equipment_Height_Code","Geo_Site_Code_POR","Geo_Site_Name_POR","Geo_City_Code_POR","Geo_City_Name_POR","Geo_Country_Code_POR","Geo_Country_Name_POR","Geo_Area_Code_POR","Geo_Area_Name_POR","Geo_Region_Code_POR","Geo_Region_Name_POR","Geo_Site_Code_POD","Geo_Site_Name_POD","Geo_City_Code_POD","Geo_City_Name_POD","Geo_Country_Code_POD","Geo_Country_Name_POD","Geo_Area_Code_POD","Geo_Area_Name_POD","Geo_Region_Code_POD","Geo_Region_Name_POD","Geo_Site_Code_LOPFI","Geo_Site_Name_LOPFI","Geo_City_Code_LOPFI","Geo_City_Name_LOPFI","Geo_Country_Code_LOPFI","Geo_Country_Name_LOPFI","Geo_Area_Code_LOPFI","Geo_Area_Name_LOPFI","Geo_Region_Code_LOPFI","Geo_Region_Name_LOPFI","Geo_Site_Code_DIPLA","Geo_Site_Name_DIPLA", "Geo_City_Code_DIPLA","Geo_City_Name_DIPLA","Geo_Country_Code_DIPLA","Geo_Country_Name_DIPLA","Geo_Area_Code_DIPLA","Geo_Area_Name_DIPLA","Geo_Region_Code_DIPLA","Geo_Region_Name_DIPLA","Geo_Country_Code_Sales_Control","Geo_Country_Name_Sales_Control","Geo_Area_Code_Sales_Control","Geo_Area_Name_Sales_Control","Geo_Region_Code_Sales_Control","Geo_Region_Name_Sales_Control","Container_Ownership_Code","Rate_Length","Rate_Length_lvl1","Rate_Mechanism","Rate_Review_Period","String","Trade_Code","Trade","Trade_Cluster","Trade_Segment","Profit_Center","String_Direction_Code","String_Direction_Name","String_Flow_Direction_Name","Trade_Market","Break_Bulk_Flag","Value_Protect_Flag","OOG_Flag","AllocatedAmountUSD","FFE_Loaded","Booked_FFE","BookedRevenue_USD","Contribution_Yield", "Activity_source_cd"]

columns = ["Cargo_Type_Name","Cargo_Type_Details_Code","Commodity_Sub_Type_Code","Contract_Product_Lvl1","Contract_Product_Lvl2",
"Contract_Product_Segment","Contract_Length","Contractual_Customer_Code","Contractual_Customer_Name","Contractual_Customer_Country_Name",
"Contractual_Customer_Area_Name","Contractual_Customer_Region_Name", "Week","Month_Unique","Quarter_Unique","Year", 
"Contractual_Customer_Concern_Code","Contractual_Customer_Concern_Name", "Contractual_Customer_Consolidated_Code","Contractual_Customer_Consolidated_Name","Contractual_Customer_Value_Proposition",
"Contractual_Customer_Vertical", "Equipment_SubType_Code","Equipment_Type_Code","Equipment_SubSize_Code","Equipment_Height_Code",
"Geo_Site_Code_POR","Geo_Site_Name_POR", "Geo_City_Code_POR","Geo_City_Name_POR","Geo_Country_Code_POR","Geo_Country_Name_POR",
"Geo_Area_Code_POR","Geo_Area_Name_POR",
"Geo_Region_Code_POR","Geo_Region_Name_POR","Geo_Site_Code_POD","Geo_Site_Name_POD","Geo_City_Code_POD","Geo_City_Name_POD",
"Geo_Country_Code_POD","Geo_Country_Name_POD","Geo_Area_Code_POD","Geo_Area_Name_POD","Geo_Region_Code_POD","Geo_Region_Name_POD",
"Geo_Site_Code_LOPFI","Geo_Site_Name_LOPFI","Geo_City_Code_LOPFI","Geo_City_Name_LOPFI","Geo_Country_Code_LOPFI","Geo_Country_Name_LOPFI",
"Geo_Area_Code_LOPFI","Geo_Area_Name_LOPFI","Geo_Region_Code_LOPFI","Geo_Region_Name_LOPFI","Geo_Site_Code_DIPLA","Geo_Site_Name_DIPLA", 
"Geo_City_Code_DIPLA","Geo_City_Name_DIPLA","Geo_Country_Code_DIPLA","Geo_Country_Name_DIPLA","Geo_Area_Code_DIPLA","Geo_Area_Name_DIPLA",
"Geo_Region_Code_DIPLA","Geo_Region_Name_DIPLA","Geo_Country_Code_Sales_Control","Geo_Country_Name_Sales_Control","Geo_Area_Code_Sales_Control",
"Geo_Area_Name_Sales_Control","Geo_Region_Code_Sales_Control","Geo_Region_Name_Sales_Control","Container_Ownership_Code","Rate_Length",
"Rate_Length_lvl1","Rate_Mechanism","Rate_Review_Period","String","Trade_Code","Trade","Trade_Cluster","Trade_Segment","Profit_Center",
"String_Direction_Code","String_Direction_Name","String_Flow_Direction_Name","Trade_Market","Break_Bulk_Flag","Value_Protect_Flag","OOG_Flag",
"AllocatedAmountUSD","FFE_Loaded","Booked_FFE","BookedRevenue_USD","Contribution_Yield", "Activity_source_cd"]

#columns = ["Geo_Region_Name_POR", "Rate_Length", "Contract_Product_Lvl1", "Trade_Code", "Year", "Month_Unique", "Week", "AllocatedAmountUSD"]

# columns = ["Week","Geo_Country_Name_POR","Year","Rate_Length_lvl1", "Rate_Length", "AllocatedAmountUSD","FFE_Loaded","Contract_Product_Lvl1",
#            "Contribution_Yield", "Contractual_Customer_Name","Geo_Region_Name_POR","String_Flow_Direction_Name","Trade_Code",
#            "Booked_FFE", "BookedRevenue_USD", "Month_Unique"]

d_types = {'Week': 'object',
'Geo_Country_Name_POR': 'category',
'Year': 'int64', 
'Rate_Length_lvl1': 'category',
'Rate_Length': 'category', 
'AllocatedAmountUSD': 'float64', 
'FFE_Loaded': 'float64', 
'Contract_Product_Lvl1': 'category', 
'Contribution_Yield': 'float64', 
'Contractual_Customer_Name': 'category', 
'Geo_Region_Name_POR': 'category',
'String_Flow_Direction_Name': 'category', 
'Trade_Code': 'category',
'Booked_FFE': 'float64',
'BookedRevenue_USD': 'float64', 
'Month_Unique': 'category'}

def data_format(df):
    if 'Month_Unique' in df.columns:
        df['Month_Unique'] = pd.to_datetime(df['Month_Unique'], format='%b %Y')
    return df

def minimize_mem_size(data_frame, temp_data_frame):
    """
    Optimizes memory usage by converting object columns to category
    only if it results in memory savings.
    """
    optimized_df = pd.DataFrame()
    col_names_exclude = ["Week","Month_Unique","Quarter_Unique","Year"]
    for col in temp_data_frame.columns :
        if (temp_data_frame[col].dtype == 'object') and (col not in col_names_exclude):
            temp_data_frame[col] = temp_data_frame[col].fillna(col+"_NA")
            orig_size = temp_data_frame[col].memory_usage(deep=True)
            cat_series = temp_data_frame[col].astype('category')
            cat_size = cat_series.memory_usage(deep=True)
            if cat_size < orig_size:
                optimized_df[col] = cat_series.copy()
            else:
                optimized_df[col] = temp_data_frame[col].copy()
        else:
            optimized_df[col] = temp_data_frame[col].copy()
    return pd.concat([data_frame, optimized_df], axis=1)
    
def df_postgres_column_batch_query(pg_host_name, pg_database_name, pg_database_uname, pg_database_pwd, schema, table, columns, batch_size=5):
    """
    Fetches data in column-wise batches from a PostgreSQL table,
    optimizes memory per batch, and returns the final DataFrame.
    """
    # Create SQLAlchemy engine
    encoded_pwd = quote_plus(pg_database_pwd)
    engine = create_engine(
        f"postgresql+psycopg2://{pg_database_uname}:{encoded_pwd}@{pg_host_name}/{pg_database_name}"
    )

    # Create column batches
    column_chunks = [columns[i:i + batch_size] for i in range(0, len(columns), batch_size)]

    final_df = pd.DataFrame()

    for i, cols in enumerate(column_chunks):
        column_str = ', '.join([f'"{col}"' for col in cols])
        sql = f'SELECT {column_str} FROM {schema}.{table}'

        # print(f"Running batch {i + 1}: {column_str}")
        logger.info(f"Running batch {i + 1}: {column_str}")
        temp_data_frame = pd.read_sql_query(sql, con=engine)

        final_df = minimize_mem_size(final_df, temp_data_frame)
    
        # print("printing the memory of full dataframe ...............")
        # print(final_df.memory_usage(deep=True).sum()/1073741824)

        logger.info("Total memory usage (in GB): %.5f", final_df.memory_usage(deep=True).sum() / 1073741824)

    logger.info("df before month unique formatting:\n%s", final_df['Month_Unique'].dtypes)
    out = final_df[final_df['Year'] == 2025].groupby(['Year', 'Month_Unique'])['AllocatedAmountUSD'].sum().diff()
    logger.info("before date format change - month on month change in rev: ", str(out))
    
    final_df = data_format(final_df)

    logger.info("df after month unique formatting:\n%s", final_df['Month_Unique'].dtypes)
    out = final_df[final_df['Year'] == 2025].groupby(['Year', 'Month_Unique'])['AllocatedAmountUSD'].sum().diff()
    logger.info("after date format change - month on month change in rev: ", str(out))

    out = final_df[(final_df['Contractual_Customer_Name'] == 'WAYFAIR') & (final_df['Contractual_Customer_Code'] == '12300046123')]
    logger.info("after date format change - WAYFAIR & 12300046123: ", str(out))

    # print("printing the memory of each column ...............")
    # print(final_df.memory_usage()/1073741824)
        
    logger.info("Memory usage by column:\n%s", final_df.memory_usage().to_string())

    logger.info("Column memory usage (in GB):\n%s", (final_df.memory_usage() / 1073741824).to_string())

    # print("printing the memory of full dataframe ...............")
    # print(final_df.memory_usage(deep=True).sum()/1073741824)

    logger.info("Total memory usage (in GB): %.5f", final_df.memory_usage(deep=True).sum() / 1073741824)

    for handler in logger.handlers:
        handler.flush()

    return final_df

def df_postgres_column_batch_query_old(pg_host_name, pg_database_name, pg_database_uname, pg_database_pwd, schema, table, columns, batch_size=5):
    """
    Fetches data in column-wise batches from a PostgreSQL table,
    optimizes memory per batch, and returns the final DataFrame.
    """
    # Create SQLAlchemy engine
    encoded_pwd = quote_plus(pg_database_pwd)
    engine = create_engine(
        f"postgresql+psycopg2://{pg_database_uname}:{encoded_pwd}@{pg_host_name}/{pg_database_name}"
    )

    column_str = ', '.join([f'"{col}"' for col in columns])

    # sql = f'SELECT {column_str} FROM {schema}.{table} LIMIT 200000'

    sql = f'SELECT * FROM {schema}.{table} LIMIT 200000'

    final_df = pd.read_sql(sql=sql, con=engine, dtype=d_types)

    #dtypes = final_df['Month_Unique'].dtypes

    #logger.info("df before month unique formatting:\n%s", final_df['Month_Unique'].dtypes)

    logger.info("Memory usage by column:\n%s", final_df.memory_usage().to_string())

    logger.info("Column memory usage (in GB):\n%s", (final_df.memory_usage() / 1073741824).to_string())

    logger.info("Total memory usage (in GB): %.5f", final_df.memory_usage(deep=True).sum() / 1073741824)

    logger.info("df after month unique formatting:\n%s", final_df['Month_Unique'].dtypes)
    out = final_df[final_df['Year'] == 2025].groupby(['Year', 'Month_Unique'], observed=True)['AllocatedAmountUSD'].sum().diff()
    logger.info("after date format change - month on month change in rev: ", str(out))

    for handler in logger.handlers:
        handler.flush()

    return final_df
