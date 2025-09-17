import pandas as pd
import asyncio
from fastapi import FastAPI, APIRouter
from src.logger import LoggingTool
from src.getllm import return_llm_obj
from src.class_finbot import FinBotClass
from src.utility import QueryRequest, apply_filters, df_postgres, read_secret_pg, df_postgres_chunked_with_engine
from src.utility_ import columns, df_postgres_column_batch_query
from src.class_to_output_format import ToOutputFormat
from src.class_finbot import create_dynamic_data_dict

#from src.data import data_dict
from src.data_dictionary import get_data_dictionary, get_sample_data
from fastapi.middleware.cors import CORSMiddleware

import threading
import gc
import os
import httpx
from datetime import datetime, date
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pytz import timezone

#os.environ["MODIN_ENGINE"] = "ray"

# Logger
logger_tool = LoggingTool(filename="finbot_test", is_console=True)
logger = logger_tool.create_and_set_logger()

# Read secrets
PG_HOST_NAME = read_secret_pg("PG_HOST_NAME")
PG_DATABASE_NAME = read_secret_pg("PG_DATABASE_NAME")
PG_DATABASE_UNAME = read_secret_pg("PG_DATABASE_UNAME")
PG_DATABASE_PWD = read_secret_pg("PG_DATABASE_PWD")
SQL_SCHEMA_NAME = read_secret_pg("SQL_SCHEMA_NAME")
SQL_DWH_TBL = read_secret_pg("SQL_DWH_TBL")

external_api_url = "https://lnschatbot-api.maersk-digital.net/api/interactions"

# App
app = FastAPI()
router = APIRouter()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Globals
df = None
finbot = None
loading = True
engine = None
load_progress = {"status": "Initializing", "pct_complete": 0}
scheduler = BackgroundScheduler()
last_refresh_time = None
refresh_lock = threading.Lock()
json_formatter = ToOutputFormat()

# Job IDs
DAILY_JOB_ID = "daily_data_refresh"
RETRY_JOB_ID = "refresh_retries"

# === ROUTES ===
@app.get("/")
def read_root():
    return {"status": "Welcome to Heartbeat Chatbot"}

@app.get("/slow")
async def slow_response():
    try:
        await asyncio.sleep(20)
        return {"msg": "Done"}
    except asyncio.CancelledError:
        print("Cancelled during sleep!")
        raise

@app.get("/healthz")
def health_check():
    return {"status": "ok"}

@app.get("/status")
def status_handler():
    global loading, load_progress, last_refresh_time
    job = scheduler.get_job(DAILY_JOB_ID)
    return {
        "loading": loading,
        "progress": {
            "status": load_progress["status"],
            "pct_complete": load_progress["pct_complete"],
            "last_refreshed": last_refresh_time.strftime("%Y-%m-%d %H:%M:%S") if last_refresh_time else "Not yet loaded",
            "next_run_time": job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job else "Unknown"
        }
    }

@app.get("/scheduler")
def check_scheduler():
    job = scheduler.get_job(DAILY_JOB_ID)
    if job:
        return {"next_run_time": job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")}
    else:
        return {"error": "Job not found"}

# === MAIN DATA LOADER ===
def preload_data():
    global df, finbot, loading, load_progress, last_refresh_time

    if refresh_lock.locked():
        logger.info("Refresh already in progress, skipping.")
        return False  # failed attempt

    with refresh_lock:
        try:
            import ray
            if ray.is_initialized():
                logger.info("Ray already initialized — shutting down and restarting.")
                ray.shutdown()
                gc.collect()

            ray.init(
                include_dashboard=False,
                ignore_reinit_error=True,
                namespace="modin_app"
            )

            # import modin.config as modin_cfg
            # modin_cfg.Engine.put("ray")
            # #import modin.pandas as pd

            logger.info("Ray and Modin initialized.")
            load_progress.update({"status": "Connecting to database...", "pct_complete": 10})
            loading = True

            logger.info("Loading data from Postgres...")
            df_loaded = df_postgres_column_batch_query(
                PG_HOST_NAME, PG_DATABASE_NAME, PG_DATABASE_UNAME,
                PG_DATABASE_PWD, SQL_SCHEMA_NAME, SQL_DWH_TBL,
                columns, batch_size=10
            )

            logger.info(f"Dynamic data dictionary is in progress.......")
            data_dict = create_dynamic_data_dict(df_loaded)
            logger.info(f"Dynamic data dictionary done!.......")

            # out = df_loaded[df_loaded['Year'] == 2025].groupby('Month_Unique')['AllocatedAmountUSD'].sum().diff()
            # logger.info("without chatengine month on month change rev: ", str(out))

            load_progress.update({"status": "Creating FinBot instance...", "pct_complete": 70})
            #finbot_obj = FinBotClass(llm=None, df=df_loaded, logger=None)
            
            llm = return_llm_obj()

            finbot_obj = FinBotClass(llm=llm, 
                     df=df_loaded, 
                     role="Distinguished Data Scientist and Pandas Expert", 
                     logger=logger, 
                     data_dictionary= data_dict,
                     sample_data=get_sample_data())

            finbot_obj.create_classes()

            # Assign after success
            globals()["df"] = df_loaded
            globals()["finbot"] = finbot_obj
            last_refresh_time = datetime.now()

            load_progress.update({"status": "Done", "pct_complete": 100})
            logger.info(f"Data refreshed at {last_refresh_time}")
            return True  # success

        except Exception as e:
            error_msg = f"Data load failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {str(e)}"
            logger.exception(error_msg)
            load_progress.update({"status": error_msg, "pct_complete": 0})
            return False  # failed attempt

        finally:
            loading = False
            for handler in logger.handlers:
                handler.flush()

# === DAILY REFRESH WITH RETRIES ===
def daily_refresh():
    logger.info("Starting daily refresh attempt...")
    success = preload_data()

    # Success → stop retry job if running
    if success and last_refresh_time and last_refresh_time.date() == date.today():
        logger.info("Daily refresh succeeded ✅ — stopping retry job if running.")
        if scheduler.get_job(RETRY_JOB_ID):
            scheduler.remove_job(RETRY_JOB_ID)
    else:
        logger.warning("Daily refresh failed — scheduling retry in 10 minutes.")
        # Schedule retry every 15 mins until success
        if not scheduler.get_job(RETRY_JOB_ID):
            scheduler.add_job(
                func=daily_refresh,
                trigger=IntervalTrigger(minutes=15, timezone=timezone("Asia/Kolkata")),
                id=RETRY_JOB_ID,
                replace_existing=True
            )

# === QUERY ENDPOINT ===
@app.post("/query")
async def query_handler(request: QueryRequest):
    global finbot, loading, load_progress

    if finbot is None:
        return {
            "response": "⚠️ FinBot is not initialized yet. Please wait until data loads at 2:00 AM IST."
        }

    if loading:
        return {
            "response": f"⚠️ Please wait. {load_progress['status']} ({load_progress['pct_complete']}%)"
        }

    try:
        query = request.query
        query_type = request.query_type
        userid = request.userid
        filters = request.filters

        chatbot_engine_response = await asyncio.to_thread(
            finbot.run_query, query=query, user_id=userid
        )

        logger.info("chat engine response:", chatbot_engine_response)

        payload = json_formatter.return_data(chatbot_engine_response, query_type, userid, query)

        logger.info("after formatting:", payload)

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                await client.post(external_api_url, json=payload)
        except Exception as log_error:
            logger.warning(f"Logging to Maersk API failed: {log_error}")

        return payload

    except asyncio.CancelledError:
        logger.warning("Request cancelled by client.")
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {"error": "Internal server error"}

# === SCHEDULER SETUP ===
@app.on_event("startup")
async def start_scheduler_only():

    logger.info("preload on startup...")
    await asyncio.to_thread(preload_data)

    # logger.info("Starting scheduler setup without preload...")
    # scheduler.add_job(
    #     func=daily_refresh,
    #     trigger=CronTrigger(hour=2, minute=0, timezone=timezone("Asia/Kolkata")),
    #     id=DAILY_JOB_ID,
    #     replace_existing=True,
    #     misfire_grace_time=None  # Run if within 1 hour late
    # )
    # scheduler.start()
    # logger.info("Scheduler started: daily data load at 2:00 AM IST with retries")

@app.on_event("shutdown")
def shutdown_ray():
    import ray
    ray.shutdown()
    scheduler.shutdown(wait=False)
    logger.info("Ray and APScheduler shut down.")

#uvicorn app.main:app --reload
#http://localhost:8000/docs