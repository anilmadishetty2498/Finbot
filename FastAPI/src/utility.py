from typing import Literal, Optional, List, Dict, Union
from pydantic import BaseModel, Field, model_validator
from langchain_experimental.agents import create_pandas_dataframe_agent
#import modin.pandas as pd
import pandas as pd
#from getllm import return_llm_obj
import asyncio
from .data_dictionary import APP_FILTERS

# from .class_finbot import FinBotClass
# from .getllm import return_llm_obj

# from .class_to_output_format import ToOutputFormat
# json_formatter = ToOutputFormat()

from sqlalchemy import create_engine
import asyncio
import threading

from urllib.parse import quote_plus
import platform
from pathlib import Path

from src.logger import LoggingTool
logger_tool = LoggingTool(filename="finbot_test", is_console=True)
logger = logger_tool.create_and_set_logger()

#llm = return_llm_obj()

class QueryRequest(BaseModel):
    query: str
    userid: str
    query_type: Literal['simple', 'table', 'graph'] = 'simple'
    
    filters: Optional[Dict[str, Union[str, List[str]]]] = Field(
        default_factory=lambda: APP_FILTERS.copy(), example=APP_FILTERS)  # 👈 use .copy() to avoid shared mutable state)

    @model_validator(mode="after")
    def validate_filters(self) -> "QueryRequest":
        """
        Ensures that if filters are provided:
        - Keys are 'region' or 'area'
        - Values are strings or lists of strings
        """
        if not self.filters:
            return self  # No filters provided, valid case

        for key, value in self.filters.items():
            if not isinstance(value, (str, list)):
                raise TypeError(f"Filter '{key}' must be a string or a list of strings.")
            if isinstance(value, list) and not all(isinstance(item, str) for item in value):
                raise TypeError(f"All items in filter '{key}' list must be strings.")
        
        return self

# def apply_filters(df: pd.DataFrame, filters: Dict[str, List[str]]) -> pd.DataFrame:
#     for col, allowed_values in filters.items():
#         if col in df.columns:
#             df = df[df[col].isin(allowed_values)]
#     return df

def apply_filters(df: pd.DataFrame, filters: Dict[str, Union[str, List[str]]]) -> pd.DataFrame:
    for col, allowed_values in filters.items():
        if col in df.columns:
            if isinstance(allowed_values, str):
                allowed_values = [allowed_values]
            df = df[df[col].isin(allowed_values)]
    return df

# def df_postgres(pg_host_name, pg_database_name, pg_database_uname, pg_database_pwd, sqlschemaName, sqlDwhTbl):

#     pg_database_pwd = quote_plus(pg_database_pwd)
    
#     #create engine
#     engine = create_engine(f"postgresql+psycopg2://{pg_database_uname}:{pg_database_pwd}@{pg_host_name}/{pg_database_name}")

#     # Load data into pandas DataFrame
#     query = f'SELECT * FROM {sqlschemaName}.{sqlDwhTbl}'

#     return pd.read_sql(query, con=engine)


engine = None

def df_postgres(pg_host_name, pg_database_name, pg_database_uname, pg_database_pwd, sqlschemaName, sqlDwhTbl):
    global engine

    if engine is None:
        encoded_pwd = quote_plus(pg_database_pwd)

        engine = create_engine(
            f"postgresql+psycopg2://{pg_database_uname}:{encoded_pwd}@{pg_host_name}/{pg_database_name}",
            pool_size=5,          # Number of connections to maintain in the pool
            max_overflow=2,       # Extra connections beyond the pool_size
            pool_timeout=30,      # Wait 30 seconds before giving up on getting a connection
            pool_recycle=1800     # Recycle connections after 30 minutes
        )

    # Load data into pandas DataFrame
    query = f'SELECT * FROM {sqlschemaName}.{sqlDwhTbl} LIMIT 10000'
    df_temp = pd.read_sql(query, con=engine)

    for col in df_temp.select_dtypes(include=['object']).columns:
        df_temp[col] = df_temp[col].astype('category')
    
    logger.info("Memory usage by column:\n%s", df_temp.memory_usage().to_string())

    logger.info("Column memory usage (in GB):\n%s", (df_temp.memory_usage() / 1073741824).to_string())
    logger.info("Total memory usage (in GB): %.5f", df_temp.memory_usage(deep=True).sum() / 1073741824)

    for handler in logger.handlers:
        handler.flush()
    return df_temp


import time

def df_postgres_chunked_with_engine(pg_host_name, pg_database_name, pg_database_uname, pg_database_pwd, schema, table):
    global engine, load_progress

    if engine is None:
        encoded_pwd = quote_plus(pg_database_pwd)
        engine = create_engine(
            f"postgresql+psycopg2://{pg_database_uname}:{encoded_pwd}@{pg_host_name}/{pg_database_name}",
            pool_size=5,
            max_overflow=2,
            pool_timeout=30,
            pool_recycle=1800
        )

    sql = f'SELECT * FROM {schema}.{table} LIMIT 3000000'
    chunksize = 100_000
    chunk_list = []
    total_rows_loaded = 0
    estimated_total_chunks = 10  # if unknown, use a guess
    cnt=1

    for i, chunk in enumerate(pd.read_sql_query(sql, con=engine, chunksize=chunksize)):
        start_time = time.time()

        for col in chunk.select_dtypes(include=['object']).columns:
             chunk[col] = chunk[col].astype('category')

        chunk_list.append(chunk)
        total_rows_loaded += len(chunk)

        end_time = time.time()
        chunk_duration = round(end_time - start_time, 2)

        pct = min(90, int(((i + 1) / estimated_total_chunks) * 100))

        load_progress = {
            "status": f"Loaded chunk {i+1} ({total_rows_loaded} rows)",
            "pct_complete": pct,
            "last_chunk_time_secs": chunk_duration
        }

        print(f"⏳ {load_progress['status']} - {pct}% complete (Chunk time: {chunk_duration}s)")
        logger.info(f"chunk {cnt}: Memory usage by column:\n%s", chunk.memory_usage().to_string())
        logger.info(f"chunk {cnt}: Column memory usage (in GB):\n%s", (chunk.memory_usage() / 1073741824).to_string())
        logger.info(f"chunk {cnt}: Total memory usage (in GB): %.5f", chunk.memory_usage(deep=True).sum() / 1073741824)
        cnt += 1

    df_temp = pd.concat(chunk_list, ignore_index=True)
    logger.info("final df memory usage by column:\n%s", df_temp.memory_usage().to_string())

    logger.info("final df Column memory usage (in GB):\n%s", (df_temp.memory_usage() / 1073741824).to_string())
    logger.info("final df Total memory usage (in GB): %.5f", df_temp.memory_usage(deep=True).sum() / 1073741824)

    for handler in logger.handlers:
        handler.flush()

    return df_temp


def read_secret_pg(name: str) -> str:
    """Reads a secret value from a file in the mounted secret directory."""
    OPENAI_SECRET_DIR = (Path("C:\tmp\pg") if platform.system() == "Windows" else Path("/etc/pg"))
    
    path = OPENAI_SECRET_DIR / name
    
    if not path.exists():
        raise FileNotFoundError(f"❌ Secret file '{name}' not found at {path}")
    
    content = path.read_text(encoding='utf-8').strip()
    if not content:
        raise ValueError(f"⚠️ Secret '{name}' is empty.")
    
    return content


# def api_output(chat_engine_response):
#     output = {
#     "answer": {
#         "code": 1,
#         "response": [chat_engine_response]
#     },
#     "prompt": {},
#     "mapping": ["map fields"] }
#     return output


def api_output(chat_engine_response, query_type):
    output = {
    "answer": {
        "code": 1,
        "response": {
            "query": chat_engine_response['query'],
            "output": json_formatter.return_data(chatbot_engine_response['output'], query_type)
            }
    },
    "prompt": {},
    "mapping": [chat_engine_response['corrected_query']]}
    return output

def load_data_in_chunks(file_path):
    chunk_list = []
    for chunk in pd.read_csv(file_path, chunksize=50000):
        chunk_list.append(chunk)
    return pd.concat(chunk_list, ignore_index=True)

        # # Run setup


# def create_finbot_agent(llm, df):
#     return create_pandas_dataframe_agent(
#         llm=llm,
#         df=df,
#         agent_type='openai-tools',
#         allow_dangerous_code=True
#     )

# async def get_simple_response(agent, query, userid):
#     #result = await agent.ainvoke(query)
#     result = await asyncio.wait_for(agent.ainvoke(query), timeout=30)
#     # print("simple output")
#     # print(userid)
#     return api_output(result)

# async def get_table_response(agent, query, userid):
#     #result = await agent.ainvoke(query)
#     result = await asyncio.wait_for(agent.ainvoke(query), timeout=30)
#     # print("table output")
#     # print(userid)
#     return api_output(result)

# async def get_graph_response(agent, query, userid):
#     # result = await agent.ainvoke(query)
#     result = await asyncio.wait_for(agent.ainvoke(query), timeout=30)
#     # print("graph output")
#     # print(userid)
#     return api_output(result)
