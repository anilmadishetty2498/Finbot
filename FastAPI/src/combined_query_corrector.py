#import modin.config as modin_cfg
import logging
#modin_cfg.Engine.put("ray")
#print(f"Current engine: {modin_cfg.Engine.get()}")
#import modin.pandas as pd
import pandas as pd
import multiprocess as mp
import os
import openai
from langchain.tools import BaseTool
from langchain.prompts import PromptTemplate
from langchain.schema import HumanMessage
from .data import data_dict
from rapidfuzz import process  
from typing import Dict, List
from pydantic import Field
import concurrent.futures
from functools import lru_cache
import pickle

# Assuming return_llm_obj is defined elsewhere
from .getllm import return_llm_obj    ######## change required to run locally src.for_test.getllm import return_llm_obj

import platform
from pathlib import Path

query_correction_prompt = PromptTemplate(
    input_variables=["user_query", "task_context"],
    template="""
You are an expert Query Correction Assistant embedded in a logistics data platform. Your role is that of a **Senior Data Scientist and Logistics Domain Expert** with deep knowledge of data schema and business context.

Your job is to **correct and standardize user queries** using the given dataframe schema. You must:
- **Preserve the user’s original intent exactly.**
- **Only modify parts necessary to match the schema.**
- **Never hallucinate columns or infer intent beyond the original query.**
- **Return consistent output across multiple runs.**

---
- You MUST always preserve and reflect the user's original question intent in the corrected query output. This means:
- The corrected query MUST start with the user's question intent (e.g., "what is", "how is", "show", "compare", etc.) followed by the corrected metrics, columns, and filters.
- You MUST NOT remove or sideline the question intent. Do not output only the metric or formula; always include the user's question phrasing.
- Example: If the user asks "what is the revenue per FFE trend for reefer", the corrected output MUST be:
    → Corrected: what is the revenue per FFE trend for reefer, where revenue is AllocatedAmountUSD, FFE is FFE_Loaded, trend is Trend is percentage change grouped by Year, REEFER in Cargo_Type_Details_Code.
- This applies to all queries. You MUST always return the corrected query in natural language, starting with the user's question intent.

You MUST make use of the following tools and information to correct the query accurately:
<<<<<<<<<<<<<<<<<<<<<<

    A. DATAFRAME SCHEMA- You must use the following data schema for query correction
    <<<<<<<<<<<<<<<<<<<
    The data schema defines all valid columns and their meanings. Use it as the only source of truth to correct or map column names.

    Cargo_Type_Name: It is the Cargo Type - e.g., 'Dry', 'Reefer', 'Unknown'
    Cargo_Type_Details_Code: It is details of Cargo type - e.g., 'DRY', 'REEFER', 'SPECIAL', 'NOR'
    Commodity_Sub_Type_Code: It is the Commodity types- e.g., 'Appliances and kitchenware', 'Seeds, beans, cereals and flour',..
    Contract_Product_Lvl1: It is the level 1 Contract Product type - e.g., 'Offline', 'Maersk Go', 'Contract Partner Products', 'Spot'
    Contract_Product_Lvl2: It is the level 2 Contract Product type - e.g., 'Offline', 'Maersk Go', 'Flexible', 'Spot', 'Block Space', 'Reef'
    Contract_Product_Segment: It is the Segment of Contract Products - e.g., 'Contracts', 'Shipments'
    Contract_Length: It is the length of the contract based on month - e.g., 'Long Term', 'Unknown', 'Short term less than 1 month', 'Short term more than 1 month'
    Contractual_Customer_Code: It is the Code of Contractual Customer (Default For Customer) - e.g., '331S1700553', '40604965725',..
    Contractual_Customer_Name: It is the Name of Contractual Customer (Default For Customer) - e.g., 'DEERE & COMPANY', 'SHENZHEN THRIVING TRADING CO.,..
    Contractual_Customer_Country_Name: It is the Country name of Contractual Customer (Default For Customer) - e.g., 'United States', 'China', 'United Arab Emirates',..
    Contractual_Customer_Area_Name: It is the Area Name of Contractual Customer (Default For Customer) - e,g., 'North America Area', 'Greater China',..
    Contractual_Customer_Region_Name: It is the Region Name of Contractual Customer (Default For Customer) - 'North America Region', 'Asia Pacific Region', 'India, Middle East, and Africa', 'Latin America Region', 'Europe Region', 'Unknown'
    Contractual_Customer_Concern_Code: It is the Concern Code of Contractual Customer - e.g.,'WW00396', 'UNK', 'WW39846'
    Contractual_Customer_Concern_Name: It is the Concern Name of Contractual Customer - e.g., 'DEERE & CO JOHN', 'Unknown', 'ENERGY COMERCIAL IMP E EXP LTDA',..
    Contractual_Customer_Consolidated_Code: It is the Consolidated Code of Contractual Customer- e.g., 'WW00396', '40604965725',..
    Contractual_Customer_Consolidated_Name: It is the Consolidated Name of Contractual Customer - e.g., DEERE & CO JOHN', 'SHENZHEN THRIVING TRADING CO.,..
    Contractual_Customer_Value_Proposition: It is the Value Proposition of Contractual Customer - e.g., 'EXPERTISE', 'EASE', 'ADVANCEMENT', 'SUPER SAVER', 'Unknown', 'EFFICIENCY', 'AMBITION'
    Contractual_Customer_Vertical: It is the Vertical of Contractual Customer - e.g., 'AUTOMOTIVE', 'RETAIL', 'TRANSPORT FREIGHT & STORAGE', 'Unknown',..
    Week: It is the Year and week of First load Date (%Y- Weeknumber) - e.g., '2023-35', '2023-03',..
    Month_Unique: It is the Month and Year of First load Date (%Y-%m-%d) - e.g., '2023-09-01', '2024-12-01','2025-05-01'..
    Quarter_Unique: It is the Quarter and Year of the First Load Date (Quarter - %Y) - e.g., 'Q3 - 2023', 'Q1 - 2023', 'Q2 - 2023', 'Q4 - 2023', 'Q1 - 2024', 'Q2 - 2024', 'Q3 - 2024', 'Q4 - 2024', 'Q1 - 2025', 'Q2 - 2025'
    Year: It is the Year of First load Date (YYYY) - e.g., '2023', '2024', '2025'
    Equipment_SubType_Code: It is the Code for Equipment sub type - e.g., 'DRY', '-1', 'TANK', 'REEF', 'OPEN', 'FLAT', 'PLWD'
    Equipment_Type_Code: It is the code for Equipment Type - e.g., 'DRY', '-1', 'REEF'
    Equipment_SubSize_Code: It is the Code for Equipment Sub size - e.g., '40.0', '-1.0', '20.0', '45.0'
    Equipment_Height_Code: It is the code for Equipment Height - '9.6"', '8.6"', '-1'
    Geo_Site_Code_POR: It is the Site code for Place of Receipt (POR) (For Export and Default if Nothing mentioned) - e.g., 'USSAVGC', 'CNIWNCT',..
    Geo_Site_Name_POR: It is the Site name for Place of Receipt (POR) (For Export and Default if Nothing mentioned) - e.g., 'Savannah Garden City Terminal L738', 'Chiwan Container Terminal Co,Ltd',..
    Geo_City_Code_POR: It is the City Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned) - e.g., 'USSAV', 'CNIWN',..
    Geo_City_Name_POR: It is the City Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned) - e.g., 'Savannah', 'Shekou', 'Jebel Ali'
    Geo_Country_Code_POR: It is the Country Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned) - e.g., 'US', 'CN', 'AE',..
    Geo_Country_Name_POR: It is the Country Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned) - e.g., 'United States', 'China', 'United Arab Emirates',..
    Geo_Area_Code_POR: It is the Area Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned) - e.g., 'NOA', 'GCA',..
    Geo_Area_Name_POR: It is the Area Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned) - e.g., 'North America Area', 'Greater China Area', 'United Arab Emirates Area',..
    Geo_Region_Code_POR: It is the Region Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned) - e.g., 'NAM', 'APA', 'IMEA', 'LAM', 'EUR', '-1'
    Geo_Region_Name_POR: It is the Region Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned) - e.g., 'North America Region', 'Asia Pacific Region', 'India, Middle East, and Africa', 'Latin America Region', 'Europe Region', 'Unknown'
    Geo_Site_Code_POD: It is the Site code for Place of Delivery (POD) (For Import) - e.g., 'BEGNK', 'YEADECT', 'DJJIBT1', 'OMSLVTM', 'BJCOOBT', 'NGON1PT', 'TRMERMT', 'MGANT'
    Geo_Site_Name_POD: It is the Site name for Place of Delivery (POD) (For Import)
    Geo_City_Code_POD: It is the City Code for Place of Delivery (POD) (For Import)
    Geo_City_Name_POD: It is the City Name for Place of Delivery (POD) (For Import)
    Geo_Country_Code_POD: It is the Country Code for Place of Delivery (POD) (For Import)
    Geo_Country_Name_POD: It is the Country Name for Place of Delivery (POD) (For Import)
    Geo_Area_Code_POD: It is the Area Code for Place of Delivery (POD) (For Import)
    Geo_Area_Name_POD: It is the Area Name for Place of Delivery (POD) (For Import)
    Geo_Region_Code_POD: It is the Region Code for Place of Delivery (POD) (For Import)
    Geo_Region_Name_POD: It is the Region Name for Place of Delivery (POD) (For Import)
    Geo_Site_Code_LOPFI: It is the Site code for First Load Port (LOPFI or FLP)
    Geo_Site_Name_LOPFI: It is the Site name for First Load Port (LOPFI or FLP)
    Geo_City_Code_LOPFI: It is the City Code for First Load Port (LOPFI or FLP)
    Geo_City_Name_LOPFI: It is the City Name for First Load Port (LOPFI or FLP)
    Geo_Country_Code_LOPFI: It is the Country Code for First Load Port (LOPFI or FLP)
    Geo_Country_Name_LOPFI: It is the Country Name for First Load Port (LOPFI or FLP)
    Geo_Area_Code_LOPFI: It is the Area Code for First Load Port (LOPFI or FLP)
    Geo_Area_Name_LOPFI: It is the Area Name for First Load Port (LOPFI or FLP)
    Geo_Region_Code_LOPFI: It is the Region Code for First Load Port (LOPFI or FLP)
    Geo_Region_Name_LOPFI: It is the Region Name for First Load Port (LOPFI or FLP)
    Geo_Site_Code_DIPLA: It is the Site code for Last Discharge Port (DIPLA or LDP)
    Geo_Site_Name_DIPLA: It is the Site name for Last Discharge Port (DIPLA or LDP)
    Geo_City_Code_DIPLA: It is the City Code for Last Discharge Port (DIPLA or LDP)
    Geo_City_Name_DIPLA: It is the City Name for Last Discharge Port (DIPLA or LDP)
    Geo_Country_Code_DIPLA: It is the Country Code for Last Discharge Port (DIPLA or LDP)
    Geo_Country_Name_DIPLA: It is the Country Name for Last Discharge Port (DIPLA or LDP)
    Geo_Area_Code_DIPLA: It is the Area Code for Last Discharge Port (DIPLA or LDP)
    Geo_Area_Name_DIPLA: It is the Area Name for Last Discharge Port (DIPLA or LDP)
    Geo_Region_Code_DIPLA: It is the Region Code for Last Discharge Port (DIPLA or LDP)
    Geo_Region_Name_DIPLA: It is the Region Name for Last Discharge Port (DIPLA or LDP)
    Geo_Country_Code_Sales_Control: It is the Country Code for Sales Control
    Geo_Country_Name_Sales_Control: It is the Country Name for Sales Control
    Geo_Area_Code__Sales_Control: It is the Area Code for Sales Control
    Geo_Area_Name_Sales_Control: It is the Area Name for Sales Control
    Geo_Region_Code_Sales_Control: It is the Region Code for Sales Control
    Geo_Region_Name_Sales_Control: It is the Region Name for Sales Control
    Container_Ownership_Code: It is the Code for Container Ownership (Owned by Maersk or Competitors)
    Rate_Length: It is the Length of the contracts basis rate exposure (Short Term and Long Term) - e.g., 'Long Term', 'Short Term'
    Rate_Length_lvl1: It is the Level 1 Details for Rate length (Also known as Modelship or rate validity) - e.g., 'Long Term Rate Contracts', 'Online Solutions', 'Short Term Rate Contracts'
    Rate_Mechanism: It is the Rate Mechanism Details - e.g., 'Contract Length >3M', 'Online Rate', 'Contract Length <1M', 'Contract Length >1M', 'Block Space'
    Rate_Review_Period: It is the Review Period for Rate lengths - e.g., 'Above 3 Months', 'Below 1 Month', 'Below 3 Months'
    String: It is the String field containing Trade related information
    Trade_Code: It is the Trade lane code
    Trade: It is the Trade lane
    Trade_Cluster: It is the Trade lane Cluster - e.g., 'NAM - North America', 'IAS - Intra Asia', 'AFR - Africa',..
    Trade_Segment: It is the segment for Trade lane
    Profit_Center: It is the Profit Centre for Trade lane
    String_Direction_Code: It is the code for direction of String or Trade - e.g., 'E', 'W', 'ME', 'R', 'AW',..
    String_Direction_Name: It is the name of direction of String or Trade - e.g., 'Eastbound', 'Westbound'
    String_Flow_Direction_Name: It is the name of the Direction flow for String or Trade - e.g., 'Backhaul', 'Headhaul', 'Unknown'
    Trade_Market: it is the Trade Market details and Direction flow - e.g., 'E-W - East-West', 'N-S - North-South',..
    Break_Bulk_Flag: It is the flag for goods transported individually, rather than in containers like big machines - e.g., 'N', 'Y'
    Value_Protect_Flag: It is a flag to indicate the Insurance for Cargo - e.g., 'N', 'Y'
    OOG_Flag: It is the Flag to identify Out of Gauge (when cargo is too large to fit inside standard shipping containers) - e.g., 'N', 'Y'
    AllocatedAmountUSD: It is the Revenue - numeric values
    FFE_Loaded: It is the Volume or FFE - numeric values
    Booked_FFE: It is the Booked Volume or FFE - numeric values
    BookedRevenue_USD: It is the Booked Revenue - numeric values
    Contribution_Yield: It is the Contribution Yield or GP - e.g., numeric values like '3402.8173828125', '0.0', '11691.373168945314',..
    >>>>>>>>>>>>>>>>>>>>

    B. TASK CONTEXT
    <<<<<<<<<<<<<<<
    You MUST use this to map business terms to their technical field names and description for consistent query interpretation.
    These interpretations should be **applied before exact column substitutions**.  
    Do **not** directly replace terms — first understand the **business intent**, then map to the appropriate schema fields accordingly.
    {task_context}
    >>>>>>>>>>>>>>>

    C. CRITICAL KEY MAPPINGS- You MUST always apply the below key mappings:
    <<<<<<<<<<<<<<<

    - `export` or `exp` → `Geo_Site_Name_POR`
    - `export code` or `exp code` → `Geo_Site_Code_POR`
    - `import` or `imp` → `Geo_Site_Name_POD`
    - `import code` or `imp code` → `Geo_Site_Code_POD`
    - `rate` → `sum(AllocatedAmountUSD)/sum(FFE_Loaded)`
    - `customer` → `Contractual_Customer_Name`
    - `company` → `Contractual_Customer_Name`
    - `customer code` → `Contractual_Customer_Code`
    - `region` → `Geo_Region_Name_POR` (unless customer region is explicitly stated)
    - `volume` → `FFE_Loaded`
    - `revenue` → `AllocatedAmountUSD`
    - `2k24` → `2024`, `2k25` → `2025`
    - `CY per FFE` → `sum(Contribution_Yield)/sum(FFE_Loaded)`
    - `modelship` → use Rate_Length_lvl1 as a grouping or segmenting column, NOT as a filter value
    - `last 4 weeks` → derive from max `Week` using format `'YYYY-WW-1'` and subtract 4 weeks using `pd.to_datetime`
    - `week` → `Week`
    - `rate` or `contract rate` → `sum(AllocatedAmountUSD)/sum(FFE_Loaded)`
    >>>>>>>>>>>>>>>

    
    D. DOMAIN KNOWLEDGE & BUSINESS TERM INTERPRETATIONS- You MUST apply these first unless overridden
    <<<<<<<<<<<<<<<
    You must use the following logistics domain-specific interpretations to correctly map business terms to schema columns. 
    These should take priority over direct keyword matching unless the user provides an explicit override.

        i. Contracting & Commercial Terminology
            - **Spot** → `Contract_Product_Lvl1`
            - **Twill** → `'Maersk Go'` in `Contract_Product_Lvl1`
            - **Contract Type (basic)** → Use one or more of: `Contract_Length`, `Rate_Length`, `Rate_Length_lvl1`
            - **Contract Type (detailed)** → Use `Rate_Mechanism` or `Rate_Review_Period` (e.g., “contract more than 3 months”)
            - **Customers/Clients** → Use `Contractual_Customer_Name`

        ii. Cargo-Type, Region, Metric, and Output Interpretation Rules
            - If query includes Cargo type (e.g., "Dry Cargo", "Special Cargo", "Reefer Cargo", "NOR"), always map to:
                → `Cargo_Type_Details_Code` (e.g., "Dry Cargo" → 'DRY', "Reefer Cargo" → 'REEFER')
                → Ignore `Cargo_Type_Name`; use `Cargo_Type_Details_Code` as default cargo filter field.

            - If the query includes a region like "in EUR", "from NAM", etc., always map the region code to:
                → `Geo_Region_Code_POR = 'XXX'` (e.g., "in EUR" → `Geo_Region_Code_POR = 'EUR'`)
                → This applies only if customer region is **not** mentioned explicitly.

            - If query says "Customers with Revenue", "per customer", or similar:
                → Output must be grouped by `Contractual_Customer_Name`.

            - Output Format Logic:
            - If the query asks for just the metric (e.g., “Dry Cargo Volume in EUR”), return a **single aggregate result**.
            - If the query is about customers (e.g., “Reefer Cargo Customers with Revenue”), return **grouped output by Contractual_Customer_Name**.
            - If the query implies trend or change over time (e.g., “Is NOR rate improving”), return **grouped by Year** and include **percentage change** in the output.


        iii. Customer Classification
            - **Customer Type or Details** (e.g., Contracts vs Shipments) → Use `Contract_Product_Segment`
            - If the query mentions "customers" (plural or singular) without a specific name or filter, then include Contractual_Customer_Name in the output projection (as a display column or grouping column).
            - If the query says “customer-wise”, “customer-level”, “by customer”, “for each customer”, etc., interpret it as a request to group by Contractual_Customer_Name.

        iv. Location Shortforms
            - **FLP / LPOFI** → First Load Port → `Geo_Site_Name_LOPFI`
            - **LDP / DIPLA** → Last Discharge Port → `Geo_Site_Name_DIPLA`
            - **POR** (Place of Receipt for Export) → `Geo_Site_Name_POR`
            - **POD** (Place of Delivery for Import) → `Geo_Site_Name_POD`

        v. Metrics & KPIs
            **Performance**:
                - Represents the change in both **Revenue** and **Volume** over time.
                - Always return all of the following in one consolidated query:  
                - sum(AllocatedAmountUSD) (Revenue)
                - percentage change in Revenue
                - sum(FFE_Loaded) (Volume)
                - percentage change in Volume
                - Group all of them by the same time column(default is `Year`, unless the user specifies Month, Week, etc.)
                - If the user compares different segments/products/customers/etc., include that grouping as well.
                - Example output:  
                "For Spot value of Contract_Product_Lvl1, what is sum(AllocatedAmountUSD), percentage change in Revenue, sum(FFE_Loaded), percentage change in Volume grouped by Year"


            **Rate**:
                - Refers to **Revenue per FFE**, i.e., `sum(AllocatedAmountUSD) / sum(FFE_Loaded)`
                - If "Contract rate" is mentioned, apply a filter: for Contracts value of Contract_Product_Segment
                - If query implies trend, improvement, or change (e.g., “improving”, “trend”, “yearly”), then include:
                    sum(AllocatedAmountUSD)/sum(FFE_Loaded), percentage change in Rate grouped by Year
                - You must always compute the rate as a single metric: sum(AllocatedAmountUSD)/sum(FFE_Loaded). Never output sum(AllocatedAmountUSD) and sum(FFE_Loaded) separately.
                - Keywords like “yearly”, “quarterly”, or “monthly” imply a time grouping by Year, Quarter_Unique, or Month_Unique respectively


            **Revenue/FFE**:
                - Defined as `AllocatedAmountUSD / FFE_Loaded`
                - If “Modelship” (`Rate_Length_lvl1`) is mentioned, group by `Rate_Length_lvl1`
                - If no grouping is mentioned, return the overall Revenue per FFE

        vi. Business Term: Modelship
            - "Modelship" is a shorthand referring to the **Rate_Length_lvl1 column**
            - It is **not a valid value** in that column.
            - When a user asks about "Modelship":
            - Group the result by `Rate_Length_lvl1`
            - Or, if trend is implied, group by both `Year` and `Rate_Length_lvl1`
            - Do NOT write `Rate_Length_lvl1 = 'Modelship'` — that is invalid.

        vii. Product Mapping
            - **Product** → Defaults to `Contract_Product_Lvl1`, unless user explicitly specifies Level 2 → `Contract_Product_Lvl2`

    >>>>>>>>>>>>>>

    E. CONTEXT-RICH QUERY FORMATION PRINCIPLES FOR PANDAS AGENTS- 
    <<<<<<<<<<<<<<<
    You MUST apply the following principles to generate semantically rich, natural language queries for pandas agent

        i. PREFERRED APPROACHES - CONTEXT-RICH FORMATS:
            - **Business-friendly language**: "for [VALUE] value of [COLUMN]" when it flows naturally
            - **Natural conjunctions**: "for [VALUE1] value of [COLUMN1] and [VALUE2] value of [COLUMN2]"
            - **Semantic clarity**: Phrases that preserve business meaning and relationships
            - **Intent preservation**: Language that clearly expresses the analytical goal
            - **Examples of effective formats**:
                - "for Spot value of Contract_Product_Lvl1" (when filtering by product type)
                - "for customers in Europe Region value of Geo_Region_Name_POR" (when location context matters)
                - "what is the revenue trend for REEFER cargo" (when business context is clear)

        ii. YOU MUST AVOID WHEN POSSIBLE THE BELOW TECHNICAL FORMATS:
            - Pure SQL syntax: "where COLUMN = 'VALUE'" (less semantic context)
            - Raw assignments: "COLUMN = 'VALUE'" (loses business meaning)
            - Technical jargon without business context


        iii. YOU MUST STRICTLY FOLLOW THE BELOW GUIDELINES FOR FLEXIBLE TRANSFORMATION:
            - **Business Context First**: Choose format that best preserves business meaning
            - **Natural Language**: Use phrasing that analysts would naturally speak
            - **Relationship Clarity**: Make column-value relationships clear in context
            - **Analytical Intent**: Ensure the query clearly expresses what analysis is needed

    >>>>>>>>>>>>>>>>
    F. TREND AND TIME RANGE HANDLING

        You must always detect trend intent and time references. Apply the following logic:

        i. TREND INTENT DETECTION
            If the query includes trend-related words (e.g., "trend", "improving", "increase", "change over time", "rising", "falling", "performance over time", "going up/down", etc.), you MUST:
            1. Recognize it as a trend query, even if no date column is mentioned explicitly.
            2. Add a time-based grouping column to reflect the trend clearly.
            3. Also calculate **percentage change over time** to clearly show whether the metric is rising or falling.
            4. Ensure that all metrics and their respective percentage changes are grouped by the same time column, and output them together in a single line. Do not return them as separate queries or clauses.
                For example, Return both the metric and its percentage change in the same line, grouped by the same time column (e.g., Year). Do not split them into separate lines or sections.

            5. Only include **percentage change over time** if the query contains **trend-related language** like:
                - “trend”, “improving”, “declining”, “change over time”, “year-over-year”, “is it going up/down”, “how is X changing”, etc.

            6. Do NOT include percentage change if the query only requests:
                - actual values (e.g., "rate", "performance", "revenue") with filters or groupings (e.g., by Year, Cargo type, Region, etc.) and **no trend language is used**.

            7. Specifically:
                - If the metric is `sum(AllocatedAmountUSD)/sum(FFE_Loaded)`:
                - Return **just this metric grouped by Year** if the query is a plain lookup (e.g., "Rate for SPECIAL Cargo in EUR")
                - Only add **percentage change** if the user asks for **improving**, **trend**, or **change over time**

            8. The output query must reflect both the main metric **and** its percentage change over time.
            
            9. Use this default **time grouping column order** (only one per query):
                - First preference → `Year`
                - If finer granularity is implied → `Quarter_Unique` or `Month_Unique`
                - Only use `Week` if explicitly mentioned (e.g., "last 4 weeks", "weekly trend")

            10. If the query compares two metrics (e.g., “is Yield in line with Revenue/FFE”), return both metrics, their percentage change over time, and group them by the same time column (e.g., Year).
            
            11. Do not return Boolean or binary outputs (e.g., true/false, 0/1).
            
            12. The goal is to enable side-by-side trend comparison, not evaluation.

        ii. USER MENTIONS A SPECIFIC DATE RANGE
            If the query includes **explicit time filters**, you must:
            - You must follow the time range mentioned by the user
            - You must add appropriate filters using the correct column (`Year`, `Month_Unique`, `Quarter_Unique`, or `Week`)
            - Still apply a grouping (if trend is implied) using one of the remaining time columns
            - Also include **percentage change** over the filtered time range


        **Example Query Conversions:**- You MUST refer the below examples for Query Correction:

            1. **User:** Is the SPOT rate improving? 
            → **Corrected:** Is the SPOT rate improving?, SPOT in Contract_Product_Lvl1, rate is sum(AllocatedAmountUSD)/sum(FFE_Loaded) percentage change grouped by Year

            2. **User:** What is the performance trend for contracts? 
            → **Corrected:** What is the performance trend for contracts?, performance is sum(AllocatedAmountUSD), sum(FFE_Loaded), trend is percentage change grouped by Year, Contracts in Contract_Product_Segment


#############
    
INSTRUCTIONS TO FOLLOW:
You MUST read the below instructions carefully and follow strictly.
    - Think carefully. First understand the query.
    - You must always apply the mappings defined in the "DOMAIN KNOWLEDGE & BUSINESS TERM INTERPRETATIONS" section before applying exact column name substitutions.
    - Never directly output raw schema column names in place of user phrases (e.g., never output "Contract_Product_Lvl1 performance").
    - Maintain natural language phrasing; corrected queries must remain grammatically readable.
    - If a term is ambiguous and cannot be mapped with confidence, leave it unchanged.
    - Avoid modifying words that are not meant to be mapped to data fields.
    - If a region or time filter is mentioned, standardize it using schema-approved values.
    - For trend-based queries, always include both the aggregated metric(s) and their percentage change over the grouped time dimension.
    - Do not split a derived metric like Rate into separate aggregates. For example, do not output sum(AllocatedAmountUSD) and sum(FFE_Loaded) if the user asked for rate. Always return the computed metric: sum(AllocatedAmountUSD)/sum(FFE_Loaded)
    - NEVER write Rate_Length_lvl1 = 'Modelship'.Instead, always use Rate_Length_lvl1 as a grouping column when the query refers to “Modelship”.
    - You **SHOULD PRIORITIZE** context-rich natural language that pandas agents can interpret effectively.
    - You MUST follow the below instructions for Output format:
        - ALWAYS start with the original question followed by a comma
        - ONLY map terms that appear in the user's question
        - You MUST NOT add any aggregation functions or SQL-like syntax
        - You MUST NOT modify the original question in any way
        - Use exact column names from the schema
        - Use "[VALUE] in [COLUMN]" format for filters when it flows naturally
        - For keywords use "[keyword] IS [COLUMN]"

        Examples:
          1. User: what is the revenue for company walmart
            → Corrected: what is the revenue for company walmart, revenue is AllocatedAmountUSD, company is Contractual_Customer_Name, walmart in Contractual_Customer_Name

          2. User: Top 5 customer by rate in 2025
            → Corrected: Top 5 customer by rate, customer is Contractual_Customer_Name, rate is sum(AllocatedAmountUSD)/sum(FFE_Loaded), 2025 in Year

#############
    

USER QUERY:
{user_query}


##############
Below are the some few shot examples that you should refer for query correction.

FEW-SHOT EXAMPLES:

    1. User: what is spot performance  
    → Corrected: what is spot performance, Spot in Contract_Product_Lvl1, performance is sum(AllocatedAmountUSD), sum(FFE_Loaded)

    2. User: what is the revenue per FFE trend for reefer
    → Corrected: what is the revenue per FFE trend for reefer, revenue is AllocatedAmountUSD, FFE is FFE_Loaded, trend is percentage change grouped by Year, REEFER in Cargo_Type_Details_Code

    3. User: How are we doing as per Modelship for Revenue
    → Corrected: How are we doing as per Modelship for Revenue, Modelship is Rate_Length_lvl1, Revenue is AllocatedAmountUSD, percentage change in AllocatedAmountUSD group by Year
    
    4. User: what is the FFE for customers in Europe region?
    → Corrected: what is the FFE for customers in Europe region?, FFE is FFE_Loaded, customers is Contractual_Customer_Name, Europe in Geo_Region_Name_POR

    5. User: what is Special Cargo performance in EUR for 2024
    → Corrected: what is Special Cargo performance in EUR for 2024, Special in Cargo_Type_Details_Code, EUR in Geo_Region_Code_POR, 2024 in Year
##############

"""
)

# Stage 2: Fuzzy Matching Tool
class QueryCorrectionTool(BaseTool):
    name: str = "QueryCorrection"
    description: str = """
    Corrects a single word by finding the closest match in a predefined dataset using fuzzy matching.
    Input: A single word to correct.
    Output: A string containing up to 5 matching column names, values, and scores (>=80) ordered by score (highest to lowest), if found, else a message indicating no matches.
    """
    data_dict: Dict[str, List[str]] = Field(..., description="Dictionary of column names to lists of unique values")
    search_index: List = Field(default_factory=list, description="Preprocessed search index")
    logger: logging.Logger = Field(default=None, description="Logger instance")

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        # Set up logger if not provided
        if self.logger is None:
            self.logger = logging.getLogger(__name__)
        # Build preprocessed search index for faster searching
        self._build_search_index()

    def _build_search_index(self):
        """Build preprocessed search index with lowercase values for faster matching"""
        self.logger.info("Building search index for fuzzy matching")
        try:
            self.search_index = []
            for key, values in self.data_dict.items():
                lowercase_values = [val.lower() for val in values]
                self.search_index.append((key, lowercase_values, values))
            self.logger.info(f"Search index built successfully with {len(self.search_index)} columns")
        except Exception as e:
            self.logger.error(f"Error building search index: {str(e)}")
            raise

    def _run(self, word: str) -> str:
        self.logger.debug(f"Running fuzzy matching for word: '{word}'")
        matches = []
        word_lower = word.lower()
        
        try:
            for key, lowercase_values, original_values in self.search_index:
                # Use rapidfuzz with score_cutoff for better performance
                output = process.extract(word_lower, lowercase_values, limit=1, score_cutoff=80)
                if output:
                    # rapidfuzz returns (match, score, index) format, not (match, score)
                    if len(output[0]) == 3:
                        match_lower, score, index = output[0]
                    else:
                        match_lower, score = output[0]
                        index = lowercase_values.index(match_lower)
                    
                    # Find original value corresponding to lowercase match
                    original_match = original_values[index]
                    matches.append((key, original_match, score))
                    self.logger.debug(f"Found match in column '{key}': '{original_match}' (score: {score})")
            
            matches = sorted(matches, key=lambda x: x[2], reverse=True)[:5]
            
            final_output = ""
            for key, match, score in matches:
                final_output += f"\nColumn '{key}' has the value '{match}' with a matching score of {score}."
            
            result = final_output if final_output else "No high-confidence matches found."
            self.logger.debug(f"Fuzzy matching result for '{word}': {len(matches)} matches found")
            return result
            
        except Exception as e:
            self.logger.error(f"Error in fuzzy matching for word '{word}': {str(e)}")
            return "Error in fuzzy matching process."

    async def _arun(self, word: str) -> str:
        return self._run(word)

fuzzy_correction_prompt = PromptTemplate(
    input_variables=["user_query", "task_context"],
    template="""
You are a Query Corrector assistant, who is expert in his field if you make mistake you will heavily penalized. Your sole task is to analyze the user query, identify words(e.g. 'spot',business names) that may be misspelled or potential values in data_dict which you think need clarification. **always call the QueryCorrection tool** for each such word.

If you perform jobs correctly then you will be rewarded with promotion and 1000$ as bonus

follow the instructions carefully and do not deviate from them.
think and rethink in clear steps then only write the query

### TASK CONTEXT
{task_context}

#######################

**Instructions:**
1. You must analyze the user query.
2. Check for the words that need clarification or correction based on the context of the query.
3. For words that might need correction, **call the QueryCorrection tool** with that word as input.
4. Return only the combined output from all tool calls. If no words need correction, return 'No corrections needed.'
5. some times customer name may be in multiple words, so if you find a word that is part of a customer name, call the QueryCorrection tool with the full customer name.(e.g kothai logistics is a customer name, so call QueryCorrection('kothai logistics') instead of 'kothai' and 'logistics' separately)

#######################

**User Query:** {user_query}

#######################

**Example:**
User Query: who all customers belong to foostuff and longteam
Tool Calls: QueryCorrection('foostuff'), QueryCorrection('longteam')
Output:
Column 'Commodity_Sub_Type_Code' has the value 'Foodstuff' with a matching score of 94.
No high-confidence matches found for 'longteam'.

User Query: Special Cargo Revenue in EUR
Tool Calls: QueryCorrection('Special Cargo'), QueryCorrection('EUR')
Output:
Column 'Cargo_Type_Details_Code' has the value 'SPECIAL' with a matching score of 95.
Column 'Geo_Region_Code_POR' has the value 'EUR' with a matching score of 90.

#######################

re-read the prompt and understand it clearly

"""

)

final_synthesis_prompt = PromptTemplate(
    input_variables=["original_query", "schema_correction", "fuzzy_correction", "task_context"],
    template="""
You are a **Final Query Synthesizer** in a logistics analytics system. Your task is to synthesize the **most accurate corrected query** by combining:
- The original user query
- Schema-based corrections (full domain knowledge)
- Fuzzy matching suggestions (high-confidence matches for values)

Your output will power downstream analytics, so it must be **semantically accurate**, **schema-compliant**, and **fully aligned with user intent**.

    ---
    ### CRITICAL INSTRUCTION: PRESERVE QUESTION INTENT
    You must always preserve and reflect the user's original question intent in the final corrected query output. This means:
    - The final corrected query must start with the user's question intent (e.g., "what is", "how is", "show", "compare", etc.) followed by the corrected metrics, columns, and filters.
    - Do not remove or sideline the question intent. Do not output only the metric or formula; always include the user's question phrasing.
    - This applies to all queries. Always return the corrected query in natural language, starting with the user's question intent.
    ---

---

## STAGE CONTEXT

- **Stage 1** provides the most context-aware correction using schema, domain rules, and semantics. Trust it as the authoritative base.
- **Stage 2** gives high-confidence fuzzy matches for possibly misspelled values or fields. Use them only when they clearly enhance Stage 1 output without changing the meaning of the query.

---

## YOUR TASK

Using all inputs provided, **synthesize a final corrected query** with:
- Logical, business-aware structure
- Correct columns/values as per schema
- Valid metrics, filters, and time aggregations
- No hallucinations or meaning changes

---

## INPUTS

Original Query:
{original_query}

Task Context:
{task_context}

Schema-Based Correction (Stage 1):
{schema_correction}

Fuzzy Matching Suggestions (Stage 2):
{fuzzy_correction}

---

##  CONTEXT-RICH QUERY SYNTHESIS PRINCIPLES FOR PANDAS AGENTS

**OBJECTIVE: SYNTHESIZE SEMANTICALLY RICH, BUSINESS-ORIENTED QUERIES FOR PANDAS AGENTS**

You **SHOULD FOCUS ON** creating natural language queries that pandas agents can interpret effectively:

#### PREFERRED SYNTHESIS APPROACHES - BUSINESS-ORIENTED:
- **Context-rich language**: "[VALUE] in [COLUMN]" when it enhances clarity
- **Natural business flow**: Queries that read like analyst conversations
- **Semantic preservation**: Maintain business relationships and intent
- **Analytical clarity**: Make the desired analysis obvious to pandas agents

**Examples of effective synthesis:**
- "Spot in Contract_Product_Lvl1" (clear product filtering)
- "SPECIAL in Cargo_Type_Details_Code, EUR in Geo_Region_Code_POR" (multiple business contexts)


####  MINIMIZE WHEN POSSIBLE - TECHNICAL SYNTAX:
- Pure SQL format: "where COLUMN = 'VALUE'" (less business context)
- Raw conditions: "COLUMN = 'VALUE'" (loses analytical intent)
- Technical jargon without business meaning


####  SYNTHESIS GUIDELINES:
- **Business Context Integration**: Merge discoveries while preserving business logic
- **Natural Language Flow**: Ensure queries read like business questions
- **Analytical Intent**: Make the desired analysis clear to pandas agents
- **Semantic Richness**: Provide context that helps code generation
- **Flexible Format**: Choose approach that best serves each specific query

**KEY GOAL: Create queries that help pandas agents understand business intent and generate effective analytical code.**

---

## SEMANTIC & STRUCTURAL RULES

1. **Do NOT add anything not present** in the original query.
2. **Do NOT alter the user’s intent or meaning**.
3. **Only fix spelling, column names, values**, and formatting.
4. Output must follow the **exact query intent**, including filters, grouping, or time granularity.
5. Output must **match business semantics**, not just literal words.
6. If query asks for **trend**, **performance**, or **change over time**, ensure proper **time-based grouping** (e.g., Year, Month_Unique).
7. If query asks for “rate” or “contract rate”, use:
   `sum(AllocatedAmountUSD)/sum(FFE_Loaded)`
   - If "contract rate", also filter: for Contracts value of Contract_Product_Segment
   - If query implies trend or change (e.g., “improving”, “change over time”, “yearly”), then return:
      e.g., sum(AllocatedAmountUSD)/sum(FFE_Loaded), percentage change in Rate grouped by Year [or appropriate time column]
   (i.e., always apply time grouping and percentage change in rate for trend queries
8. For “performance”:
   - Always return both `sum(AllocatedAmountUSD)` and `sum(FFE_Loaded)`
   - Also include their **percentage change over time**
   - Group them by one consistent time column (prefer `Year`)
   - Output as one consolidated line, not split
9. If returning **multiple metrics with trends**, apply a **single `grouped by` clause at the end**, not per metric.
   -  Good: `sum(A), percentage change in A, sum(B), percentage change in B grouped by Year`
   -  Bad: `sum(A), percentage change in A grouped by Year, sum(B)...`
10. Phrases like “how is X trending”, “is it improving”, “performance over time”, “change in volume” imply a trend intent.
    - Add time grouping and % change to applicable metrics
11. Never output sum(AllocatedAmountUSD) and sum(FFE_Loaded) separately if the intent is to compute rate. Always return sum(AllocatedAmountUSD)/sum(FFE_Loaded) as a single metric.
12. Never output Rate_Length_lvl1 = 'Modelship'. Modelship is not a value. Always group by Rate_Length_lvl1 when Modelship is mentioned.

---

## SMART FUZZY MATCHING LOGIC (Stage 2 Integration)

If multiple fuzzy matches are returned:

1. **Do not blindly pick the highest score**.
2. Evaluate **logical fit** of each match with original query context.
3. Prefer matches whose **column** aligns with query topic:
   - Query mentions “cargo” → prefer `Cargo_Type_Details_Code`
   - Query mentions “spot” or “contract” → prefer `Contract_Product_Lvl1`, `Rate_Length`, `Rate_Mechanism`
   - Query mentions “customer” → prefer `Contractual_Customer_Name`, `Contractual_Customer_Code`
   - Query mentions “region”, “location” → prefer `Geo_Region_Name_POR`
4. Only use fuzzy matches with **score ≥ 90**, unless nothing better exists.
5. Disregard fuzzy matches for company names unless user asks for specific customer.
6. Ignore any fuzzy match that **conflicts** with schema-corrected result unless it **clearly fixes a real mistake**.
7. If a fuzzy match contradicts the schema correction and doesn't clearly fix an error, **trust Stage 1** over Stage 2.

---

### You must follow below critical rules Ffor finding value of Contractual_Customer_Name column
>>>>>>>>>>
When a fuzzy match finds a company name in the Contractual_Customer_Name column:
1. You must always insert the exact database value found in Contractual_Customer_Name (e.g., "NIKE INTL LTD") 
2. You must use the exact casing and formatting from the database for the column Contractual_Customer_Name.
3. You must never use the original user input (e.g., "Nike")
4. You must never use the values from other columns like Contractual_Customer_Concern_Name (e.g., "NIKE" from Contractual_Customer_Concern_Name)
5. Apply this rule even if Stage 1 correction already included a customer filter

Example:
- User wrote: "Nike revenue"
- Fuzzy match results:
    Column 'Contractual_Customer_Concern_Name' has the value 'NIKE' with a matching score of 100.0.
    Column 'Contractual_Customer_Consolidated_Name' has the value 'NIKE' with a matching score of 100.0.
    Column 'Contractual_Customer_Name' has the value 'NIKE INTL LTD' with a matching score of 90.0.
- Fuzzy match found: "NIKE INTL LTD" in Contractual_Customer_Name
- CORRECT: NIKE INTL LTD in Contractual_Customer_Name
- INCORRECT: for NIKE value of Contractual_Customer_Name
- INCORRECT: for Nike value of Contractual_Customer_Name

## DOMAIN-AWARE MAPPINGS TO APPLY

| Query Word       | Column / Logic                                  |
|------------------|--------------------------------------------------|
| spot             | for Spot value of Contract_Product_Lvl1                  |
| reefer           | for REEFER value of Cargo_Type_Details_Code              |
| rate             | sum(AllocatedAmountUSD)/sum(FFE_Loaded)         |
| revenue          | AllocatedAmountUSD                              |
| volume           | FFE_Loaded                                      |
| performance      | sum(AllocatedAmountUSD), sum(FFE_Loaded)        |
| contract rate    | Add filter: for Contracts value of Contract_Product_Segment |
| last 4 weeks     | Derive using max(Week) in format YYYY-WW-1 and subtract 4 weeks |
| modelship        | Rate_Length_lvl1  (group by — not value filter)                              |
|is X in line with Y, does X match Y, is increase in A aligned with B | Return both A and B metrics with % change over time grouped by Year |
|EUR             | for EUR value of Geo_Region_Code_POR                     |
  |

---

## HIERARCHY RULES

1. Default Product field → `Contract_Product_Lvl1`
2. Default Location → `Geo_Region_Name_POR` (unless user mentions Country/City)
3. Only use **one time grouping per query**:
   - First: `Year`
   - Then: `Quarter_Unique` (if finer granularity implied)
   - Then: `Month_Unique` (if explicitly stated)
   - Use `Week` **only** if query contains “weekly”, “last x weeks”, or similar

---

## QUERY SYNTHESIS RULES

Please follow these rules when synthesizing the final query:

- Use `Cargo_Type_Details_Code` as the canonical field for all cargo type filters.
- Apply `Geo_Region_Code_POR` mapping when region like “in EUR” is mentioned.
- If query involves “Customers with…” or “per customer”, group output by `Contractual_Customer_Name`.
- Volume → `FFE_Loaded`, Revenue → `AllocatedAmountUSD`, Rate → `sum(AllocatedAmountUSD)/sum(FFE_Loaded)`
- Choose output type based on query:
  - Aggregate result if it’s a general metric query.
  - Grouped result if it's customer-based.
  - Group by `Year` and compute percentage change for trend questions.

---

## FINAL OUTPUT RULES

- Only return the final corrected query  
- No explanation, comments, or formatting  
- Do NOT prefix with “Corrected Query:”  
- No quotes or markdown  
- Just the corrected query text  
- Always maintain this metric order (if present):
  1. Metric
  2. % Change
  3. Grouping
- Output as one single line query. Avoid splitting per metric.

---

## EXAMPLES

1. **Query:** what is Special Cargo Revenue  
    → output: what is Special Cargo Revenue, SPECIAL in Cargo_Type_Details_Code, revenue is AllocatedAmountUSD

2. **Query:** Is Spot rate improving  
    → output: Is the SPOT rate improving?, SPOT in Contract_Product_Lvl1, rate is sum(AllocatedAmountUSD)/sum(FFE_Loaded) percentage change grouped by Year

3. **Query:** What Customer-wise performance  
    → output:  What Customer-wise performance, Customer is Contractual_Customer_Name, performance is sum(AllocatedAmountUSD), sum(FFE_Loaded)

4. **Query:** What is Special Cargo Rate in EUR?  
    → output: What is Special Cargo Rate in EUR?, SPECIAL in Cargo_Type_Details_Code, Rate is sum(AllocatedAmountUSD)/sum(FFE_Loaded), EUR in Geo_Region_Code_POR


---

Re-read the original query and compare both Stage 1 and Stage 2 corrections. Think carefully and then produce the best possible version of the query using all the rules above.

Final Corrected Query:
"""
)


class CombinedQueryCorrector:
    def __init__(self, data_dict: Dict[str, List[str]] = None, llm=None, df=None, logger=None):
        
        """
        Initialize the combined query corrector
        
        Args:
            data_dict: Pre-built dictionary of column names to lists of unique values
            df: DataFrame to build data_dict from (alternative to data_dict)
            cache_file: File to cache preprocessed search index
            logger: Logger instance

        flow:
        1. Initialize logger
        2. If data_dict is provided, use it directly; if df is provided,    
              build data_dict from DataFrame
        3. Try to load cached search index from cache_file
        4. If cache is valid, use it; otherwise, build new search index 
        5. Initialize QueryCorrectionTool with data_dict and logger def_intialize_query_tool
        6. Create a simple cache dictionary for fuzzy matching results
        7. Initialize LLM and bind tools
        def run_schema_correction
        8. Run schema-based correction using LLM with query_correction_prompt
        def run_fuzzy_correction
        9. Run fuzzy matching correction using LLM with fuzzy_correction_prompt
        def cached_tool_run
        10. Use a simple cache for fuzzy matching results to avoid redundant calls if word not found in cached results
        def _initialize_query_tool
        def run_final_synthesis
        11. Run final synthesis using LLM with final_synthesis_prompt
        def correct_query
        12. Main method to correct user query by running schema correction, fuzzy correction, and final synthesis in parallel
        13. Returns the final corrected query

        """
        # Set up logger
        if logger is None:
            self.logger = logging.getLogger(__name__)
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger
            
        self.logger.info("Initializing CombinedQueryCorrector")
        self.task_context = ""
        
        self.cache_file = None
        
        try:
            if data_dict is not None:
                self.logger.info("Using provided data_dict")
                self.data_dict = data_dict
            elif df is not None:
                self.logger.info("Building data_dict from DataFrame")
                self.data_dict = {
                    col: df[col].dropna().astype(str).unique().tolist()
                    for col in df.columns
                    if df[col].dtype in ['object', 'category']
                }
                self.logger.info(f"Built data_dict with {len(self.data_dict)} columns")
            else:
                self.logger.error("Neither data_dict nor df was provided")
                raise ValueError("Either data_dict or df must be provided")
            
            # Try to load cached search index
            self.query_tool = self._initialize_query_tool()
            # Create a simple cache dictionary instead of lru_cache for ProcessPoolExecutor compatibility
            self.fuzzy_cache = {}
            # self.llm = return_llm_obj()
            self.llm = llm
            self.llm_with_tools = self.llm.bind_tools([self.query_tool], tool_choice="auto")
            self.logger.info("CombinedQueryCorrector initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing CombinedQueryCorrector: {str(e)}")
            raise

    def _initialize_query_tool(self):
        """Initialize query tool with caching support"""
        self.logger.info("Initializing QueryCorrectionTool")
        try:
            # Try to load cached search index
            if os.path.exists("lns"):
                with open(self.cache_file, 'rb') as f:
                    cached_data = pickle.load(f)
                    if cached_data.get('data_dict') == self.data_dict:
                        self.logger.info("Loading cached search index...")
                        tool = QueryCorrectionTool(data_dict=self.data_dict, logger=self.logger)
                        tool.search_index = cached_data['search_index']
                        return tool
        except (FileNotFoundError, KeyError, pickle.PickleError) as e:
            self.logger.warning(f"Could not load cached search index: {str(e)}")
        
        # Create new tool and cache the search index
        self.logger.info("Building new search index...")
        tool = QueryCorrectionTool(data_dict=self.data_dict, logger=self.logger)
        
        try:
            cache_data = {
                'data_dict': self.data_dict,
                'search_index': tool.search_index
            }
            with open(self.cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
            self.logger.info("Search index cached successfully.")
        except Exception as e:
            self.logger.warning(f"Warning: Could not cache search index: {e}")
        
        return tool

    def run_schema_correction(self, user_query: str) -> str:
        """Stage 1: Schema-based correction
        Args:
            user_query: The original user query to correct
        Returns:
            str: The corrected query based on schema mapping    
        """
        self.logger.info("Running Stage 1: Schema-based correction")
        try:
            formatted_prompt = query_correction_prompt.format(
                user_query=user_query,
                task_context=self.task_context
                )
            response = self.llm.invoke(formatted_prompt)
            result = response.content
            self.logger.info(f"Schema correction completed: '{result}'")
            return result
        except Exception as e:
            self.logger.error(f"Schema correction error: {str(e)}")
            return f"Schema correction error: {str(e)}"

    def _cached_tool_run(self, word: str) -> str:
        """Simple cache implementation compatible with ProcessPoolExecutor
        Args:
            word: The word to correct using the QueryCorrection tool
        Returns:
            str: The corrected word or a message indicating no matches found
        """
        if word in self.fuzzy_cache:
            self.logger.debug(f"Using cached result for word: '{word}'")
            return self.fuzzy_cache[word]
        
        result = self.query_tool._run(word)
        self.fuzzy_cache[word] = result
        return result

    def run_fuzzy_correction(self, user_query: str) -> str:
        """
        Optimized Stage 2 with parallel processing and caching
        Args:
            user_query: The original user query to correct
        Returns:
            str: The corrected query based on fuzzy matching of words

        """
        self.logger.info("Running Stage 2: Fuzzy matching correction")
        print("log0")
        try:
            print("log0.1")
            prompt = fuzzy_correction_prompt.format(user_query=user_query, task_context=self.task_context)
            print("log0.2")
            i=0
            while i<4:
                try:
                    response = self.llm_with_tools.invoke([HumanMessage(content=prompt)])
                    break
                except Exception as e:
                    print("Error occurred:", e)
                    i+=1

            print("log0.3")


            # Uses the LLM with tools bound (self.llm.bind_tools([self.query_tool]))
            # Requires a list of message objects (not a plain string)
            # This is because tool-enabled LLMs need to track conversation context for tool calling
            
            if hasattr(response, 'tool_calls') and response.tool_calls:
                # Extract words first
                words = []
                for tool_call in response.tool_calls:
                    print("log1")
                    if tool_call['name'] == 'QueryCorrection':
                        print("log2")
                        words.append(tool_call['args']['word'])
                
                self.logger.info(f"Running fuzzy correction for {len(words)} words: {words}")
                
                # Use ThreadPoolExecutor instead of ProcessPoolExecutor to avoid pickling issues
                # but still get parallelization benefits for I/O bound operations
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    print("log3")
                    futures = [executor.submit(self._cached_tool_run, word) for word in words]
                    print("log4")
                    results = [f.result() for f in concurrent.futures.as_completed(futures)]
                
                result = "\n".join(results)
                print("log5")
                self.logger.info("Fuzzy correction completed")

                return result
                
            self.logger.info("No fuzzy corrections needed")
            print("log6")
            return "No fuzzy corrections needed."
        except Exception as e:
            self.logger.error(f"Fuzzy correction error: {str(e)}")
            return f"Fuzzy correction error: {str(e)}"
    
    def run_final_synthesis(self, original_query: str, schema_result: str, fuzzy_result: str) -> str:
        """Stage 3: Final synthesis
        Args:
            original_query: The original user query
            schema_result: The result from schema-based correction
            fuzzy_result: The result from fuzzy matching correction

        Returns:
            str: The final synthesized query combining schema and fuzzy results
        
        """
        self.logger.info("Running Stage 3: Final synthesis")
        try:
            formatted_prompt = final_synthesis_prompt.format(
                original_query=original_query,
                schema_correction=schema_result,
                fuzzy_correction=fuzzy_result,
                task_context=self.task_context
            )
            response = self.llm.invoke(formatted_prompt)
            result = response.content
            self.logger.info(f"Final synthesis completed: '{result}'")
            return result
        except Exception as e:
            self.logger.error(f"Final synthesis error: {str(e)}")
            return f"Final synthesis error: {str(e)}"

    def correct_query(self, user_query: str, verbose: bool = True) -> str:
        """Optimized main method with parallel stage execution
        Args:
            user_query: The original user query to correct
            verbose: Whether to print detailed output during the correction process
        Returns:
            str: The final corrected query after all stages of correction
        
        """
        self.logger.info(f"Starting query correction for: '{user_query}'")
        
        if verbose:
            print(f"Original Query: {user_query}\n{'-'*50}")
        
        # Run Stage 1 and Stage 2 in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            schema_future = executor.submit(self.run_schema_correction, user_query)
            fuzzy_future = executor.submit(self.run_fuzzy_correction, user_query)
            schema_result, fuzzy_result = schema_future.result(), fuzzy_future.result()
        
        if verbose:
            print(f"Stage 1 (Schema): {schema_result}\n{'-'*50}")
            print(f"Stage 2 (Fuzzy): {fuzzy_result}\n{'-'*50}")
        
        # Stage 3: Final synthesis
        final_result = self.run_final_synthesis(user_query, schema_result, fuzzy_result)
        if verbose:
            print(f"Stage 3 (Final): {final_result}\n{'='*50}")
        
        self.logger.info(f"Query correction completed. Final result: '{final_result.strip()}'")
        return final_result.strip()

    def set_task_context(self, task_context: str):
        """
        Set the task context (e.g., task description) for use in query correction.
        """
        self.task_context = task_context