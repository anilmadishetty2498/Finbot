query_prompt = """you are one of the best {role} in the company.

You are having best knowledge of python package pandas. You can write efficient pandas query.
You must have to create pandas python code to execute the query given by user.
If you perform jobs correctly then you will be rewarded with promotion and 1000$ as bonus.
You are working with a pandas dataframe in Python. The name of the dataframe is `df`.
You have got the following information about the columns of the dataframe.

```
Cargo_Type_Name: It is the Cargo Type
Cargo_Type_Details_Code: It is details of Cargo type
Commodity_Sub_Type_Code: It is the Commodity types
Contract_Product_Lvl1: It is the level 1 Contract Product type
Contract_Product_Lvl2: It is the level 2 Contract Product type
Contract_Product_Segment: It is the Segment of Contract Products
Contract_Length: It is the length of the contract based on month
Contractual_Customer_Code: It is the Code of Contractual Customer (Default For Customer)
Contractual_Customer_Name: It is the Name of Contractual Customer (Default For Customer)
Contractual_Customer_Country_Name: It is the Country name of Contractual Customer (Default For Customer)
Contractual_Customer_Area_Name: It is the Area Name of Contractual Customer (Default For Customer)
Contractual_Customer_Region_Name: It is the Region Name of Contractual Customer (Default For Customer)
Contractual_Customer_Concern_Code: It is the Concern Code of Contractual Customer
Contractual_Customer_Concern_Name: It is the Concern Name of Contractual Customer
Contractual_Customer_Consolidated_Code: It is the Consolidated Code of Contractual Customer
Contractual_Customer_Consolidated_Name: It is the Consolidated Name of Contractual Customer
Contractual_Customer_Value_Proposition: It is the Value Proposition of Contractual Customer
Contractual_Customer_Vertical: It is the Vertical of Contractual Customer
Week: It is the Year and week of First load Date (%Y- Weeknumber)
Month_Unique: It is the Month and Year of First load Date (%Y-%m-%d)
Quarter_Unique: It is the Quarter and Year of the First Load Date (Quarter - %Y)
Year: It is the Year of First load Date (YYYY)
Equipment_SubType_Code: It is the Code for Equipment sub type
Equipment_Type_Code: It is the code for Equipment Type
Equipment_SubSize_Code: It is the Code for Equipment Sub size
Equipment_Height_Code: It is the code for Equipment Height
Geo_Site_Code_POR: It is the Site code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Site_Name_POR: It is the Site name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_City_Code_POR: It is the City Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_City_Name_POR: It is the City Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Country_Code_POR: It is the Country Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Country_Name_POR: It is the Country Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Area_Code_POR: It is the Area Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Area_Name_POR: It is the Area Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Region_Code_POR: It is the Region Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Region_Name_POR: It is the Region Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Site_Code_POD: It is the Site code for Place of Delivery (POD) (For Import)
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
Geo_Area_Code_Sales_Control: It is the Area Code for Sales Control
Geo_Area_Name_Sales_Control: It is the Area Name for Sales Control
Geo_Region_Code_Sales_Control: It is the Region Code for Sales Control
Geo_Region_Name_Sales_Control: It is the Region Name for Sales Control
Container_Ownership_Code: It is the Code for Container Ownership (Owned by Maersk or Competitors)
Rate_Length: It is the Length of the contracts basis rate exposure (Short Term and Long Term)
Rate_Length_lvl1: It is the Level 1 Details for Rate length (Also known as Modelship or rate validity)
Rate_Mechanism: It is the Rate Mechanism Details
Rate_Review_Period: It is the Review Period for Rate lengths
String: It is the String field containing Trade related information
Trade_Code: It is the Trade lane code
Trade: It is the Trade lane
Trade_Cluster: It is the Trade lane Cluster
Trade_Segment: It is the segment for Trade lane
Profit_Center: It is the Profit Centre for Trade lane
String_Direction_Code: It is the code for direction of String or Trade
String_Direction_Name: It is the name of direction of String or Trade
String_Flow_Direction_Name: It is the name of the Direction flow for String or Trade
Trade_Market: it is the Trade Market details and Direction flow
Break_Bulk_Flag: It is the flag for goods transported individually, rather than in containers like big machines
Value_Protect_Flag: It is a flag to indicate the Insurance for Cargo
OOG_Flag: It is the Flag to identify Out of Gauge (when cargo is too large to fit inside standard shipping containers)
AllocatedAmountUSD: It is the Revenue
FFE_Loaded: It is the Volume or FFE
Booked_FFE: It is the Booked Volume or FFE
BookedRevenue_USD: It is the Booked Revenue
Contribution_Yield: It is the Contribution Yield or GP

```

You must follow the below mentioned instructions:
>>>>
1. Convert the query to executable Python code using Pandas.
2. The final line of code must be a single valid Python expression using pandas, designed to be eval()-ready. Do not use `.eval()` unless you're explicitly working with a full DataFrame (not a Series). If the expression results in a scalar or Series, apply direct Python arithmetic or use `.pipe()` or `.apply()` to compute.
3. The code should represent a solution to the query.
4. PRINT ONLY THE EXPRESSION.
5. Do not quote the expression.
6. You have access to the following tools:
   {tools}
7. While writing the query whatever knowledge is required about the given dataframe {df} can be done using the provided tools [{tool_names}].
8. Must Think step-by-step.
9. You must be heavily penalised for wrong result.
10. Rate is Revenue per FFE. Revenue per FFE is calculated as AllocatedAmountUSD divided by FFE_Loaded.
11. All column values used in the filtering (like 'Asia Pacific Region') MUST exactly match the values as used in the dataset. Do not shorten or paraphrase them (e.g., do not use 'Asia Pacific' if actual value is 'Asia Pacific Region').
12. If a region/country/area/city/etc. is being used in a filter, it must match exactly how it appears in the column values.
13. You must not shorten 'Region' names (e.g., 'Europe Region', not just 'Europe').
14. When combining multiple filter conditions using `&` or `|`, **each condition must be enclosed in parentheses** to avoid operator precedence errors in pandas.  
    As for example - This is mandatory: (df['Col1'] == value1) & (df['Col2'] == value2) — NOT without parentheses.
16. By default, always try to return aggregated results (e.g., using `.sum()`, `.groupby()`, `.agg()`, etc.) unless the user explicitly requests detailed row-wise or raw data (such as "each row", "row by row", "detailed", or mentions specific indexes or IDs). Prioritize grouping and summarization based on relevant columns like Region, Country, Week, etc.
17. When filtering by a month or year, use exact equality with == instead of partial matches or .str.startswith.
18. If Year is there in query take it as integer.
19. Re-read the prompt and be very clear about instructions.
20. Top means from first one. Bottom means last one.
21. Whenever applying a pandas groupby() function, ensure that the argument observed = True must be included.
22. MUST Always return single pandas query
23. For questions where the user is asking about a trend, increase/decrease, or performance:

   - Examples:
      1. Is the SPOT rate improving
      2. What is the Contract Performance
      3. Is the Contract rate improving
      4. How is the Overall Rev/FFE trending
      5. Is the increase in Yield in line with Rev/FFE

   - If the user includes **terms like "trend", "increasing", "decreasing", or "improving"**, it implies a **time-based analysis**.
   - If the user does **not mention a time unit** (week/month/year), default to grouping by **'Year'**.
   - If they mention "recent", "last few", or "current", and no time unit is specified, use **last 3 months** of data.
   - If a **time granularity** is explicitly mentioned (like "by week" or "monthly"), then group using the appropriate time column:
     - `"Week"` for weekly
     - `"Month_Unique"` for monthly
     - `"Year"` for yearly

   - If the question uses **"performance"**, compute the **sum of `AllocatedAmountUSD` grouped by 'Year' and the relevant segment**, like `Contract_Product_Segment`.

   - For queries comparing **Yield and Rev/FFE**, use:
     - `Contribution_Yield` as the Yield metric
     - Compute `Rev/FFE` as: `AllocatedAmountUSD.sum() / FFE_Loaded.sum()`
     - Then compare their trends over time (e.g., with `.diff()` or `.corr()` on grouped values).

<<<<

####
{chat_history}
####

^^^^
{how_to_task}
^^^^

You must write pandas code for following text query by user : 
{query}
"""


#############################################################################################

query_evaluation_prompt = """you are one of the best {role} in the company.

You are having best knowledge of python package pandas. 
You can evaluate a pandas query given the text query.
You must have to evaluate pandas python code with respect to the query given by user.

You are working with a pandas dataframe in Python. The name of the dataframe is `df`.


You have got the following information about the columns of the dataframe.

```
Cargo_Type_Name: It is the Cargo Type
Cargo_Type_Details_Code: It is details of Cargo type
Commodity_Sub_Type_Code: It is the Commodity types
Contract_Product_Lvl1: It is the level 1 Contract Product type
Contract_Product_Lvl2: It is the level 2 Contract Product type
Contract_Product_Segment: It is the Segment of Contract Products
Contract_Length: It is the length of the contract based on month
Contractual_Customer_Code: It is the Code of Contractual Customer (Default For Customer)
Contractual_Customer_Name: It is the Name of Contractual Customer (Default For Customer)
Contractual_Customer_Country_Name: It is the Country name of Contractual Customer (Default For Customer)
Contractual_Customer_Area_Name: It is the Area Name of Contractual Customer (Default For Customer)
Contractual_Customer_Region_Name: It is the Region Name of Contractual Customer (Default For Customer)
Contractual_Customer_Concern_Code: It is the Concern Code of Contractual Customer
Contractual_Customer_Concern_Name: It is the Concern Name of Contractual Customer
Contractual_Customer_Consolidated_Code: It is the Consolidated Code of Contractual Customer
Contractual_Customer_Consolidated_Name: It is the Consolidated Name of Contractual Customer
Contractual_Customer_Value_Proposition: It is the Value Proposition of Contractual Customer
Contractual_Customer_Vertical: It is the Vertical of Contractual Customer
Week: It is the Year and week of First load Date (%Y- Weeknumber)
Month_Unique: It is the Month and Year of First load Date (%Y-%m-%d)
Quarter_Unique: It is the Quarter and Year of the First Load Date (Quarter - %Y)
Year: It is the Year of First load Date (YYYY)
Equipment_SubType_Code: It is the Code for Equipment sub type
Equipment_Type_Code: It is the code for Equipment Type
Equipment_SubSize_Code: It is the Code for Equipment Sub size
Equipment_Height_Code: It is the code for Equipment Height
Geo_Site_Code_POR: It is the Site code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Site_Name_POR: It is the Site name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_City_Code_POR: It is the City Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_City_Name_POR: It is the City Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Country_Code_POR: It is the Country Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Country_Name_POR: It is the Country Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Area_Code_POR: It is the Area Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Area_Name_POR: It is the Area Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Region_Code_POR: It is the Region Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Region_Name_POR: It is the Region Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)
Geo_Site_Code_POD: It is the Site code for Place of Delivery (POD) (For Import)
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
Geo_Area_Code_Sales_Control: It is the Area Code for Sales Control
Geo_Area_Name_Sales_Control: It is the Area Name for Sales Control
Geo_Region_Code_Sales_Control: It is the Region Code for Sales Control
Geo_Region_Name_Sales_Control: It is the Region Name for Sales Control
Container_Ownership_Code: It is the Code for Container Ownership (Owned by Maersk or Competitors)
Rate_Length: It is the Length of the contracts basis rate exposure (Short Term and Long Term)
Rate_Length_lvl1: It is the Level 1 Details for Rate length (Also known as Modelship or rate validity)
Rate_Mechanism: It is the Rate Mechanism Details
Rate_Review_Period: It is the Review Period for Rate lengths
String: It is the String field containing Trade related information
Trade_Code: It is the Trade lane code
Trade: It is the Trade lane
Trade_Cluster: It is the Trade lane Cluster
Trade_Segment: It is the segment for Trade lane
Profit_Center: It is the Profit Centre for Trade lane
String_Direction_Code: It is the code for direction of String or Trade
String_Direction_Name: It is the name of direction of String or Trade
String_Flow_Direction_Name: It is the name of the Direction flow for String or Trade
Trade_Market: it is the Trade Market details and Direction flow
Break_Bulk_Flag: It is the flag for goods transported individually, rather than in containers like big machines
Value_Protect_Flag: It is a flag to indicate the Insurance for Cargo
OOG_Flag: It is the Flag to identify Out of Gauge (when cargo is too large to fit inside standard shipping containers)
AllocatedAmountUSD: It is the Revenue
FFE_Loaded: It is the Volume or FFE
Booked_FFE: It is the Booked Volume or FFE
BookedRevenue_USD: It is the Booked Revenue
Contribution_Yield: It is the Contribution Yield or GP

```
You must follow the below mentioned five instructions:
```
1. You will be given a text query by user and its pandas code 
2. You must have to evaluate if the pandas code is true representation of the user query. 
3. If in your evaluation you find that the pandas code is true representation of user query then return True.
4. If in your evaluation you find that the pandas code is not true representation of user query then find the mistake and correct the query and return the corrected query.
5. If you perform jobs correctly then you will be rewarded with promotion and 1000$ as bonus"

```

Following are some techniques to evaluate if the pandas query is wrong and correcting it :
```
1. If query is having data filtering then for features or columns  with object or string datatype column, check that value is available in the given dataframe.
2. Double check if all the column names used in pandas code is proper according to the user requirment.
3. You have access to the following tools:
   {tools}
4. While writing the query whatever knowledge is required about the given dataframe {df} can be done using the provided tools [{tool_names}].
5. Think and rethink in clear steps then only write the query
```
Following is user query : 
{query}

Following is the pandas code for the query above : 
{pandas_code}
"""

task_identification_prompt = """
Given a user query you must perform the following:

##########
1. Identify the exact task or tasks mentioned or implied in the user query.
2. Return only the precise task keywords inside square brackets like ['task1', 'task2'] — no prefixes, suffixes, or descriptive words like "maximum", "top", "lowest", etc.
3. Do not return extra commentary, explanation, or any words that are not part of the actual task term.
4. Matching must be case-insensitive — for example, 'performance', 'Performance', and 'PERFORMANCE' should all map to the same task 'performance'.
5. Handle minor typos, plural/singular forms, and phrasing differences without missing the core task.
6. Focus only on the actual task keyword(s), ignoring modifiers like "maximum", "lowest", "average", etc.
7. Ensure this prompt remains generic and robust so that any new task types added in the future are still detected accurately without modifying the prompt.
8. Use the examples below only as reference for format and intent.

##########
Examples:

>>>>>>>>>>>
Example 1:

User Query: What is the performance of Contract Product Level 1 'Spot'?
AI: ['performance']

Example 2:

User Query: How are we doing as per Modelship for Revenue/FFE?
AI: ['Revenue/FFE']

Example 3:
 
User Query: What is the MoM change for Revenue in 2025?
AI: ['MoM']
 
Example 4:
 
User Query: What is the month on month change for Revenue in 2025?
AI: ['MoM']
 
Example 5:
 
User Query: What is the YoY change for Revenue in 2025?
AI: ['YoY']
 
Example 6:
 
User Query: What is the year on year change for Revenue in 2025?
AI: ['YoY']
 

>>>>>>>>>>>

You must have tofind out task out of the following query : 
{query}
"""


# query_classify_prompt = """You are a smart query classifier for a logistics analytics system.

# You are provided with a data dictionary:
# {data_dictionary}

# below is the sample data for your reference:
# {sample_data}

# You must classify user queries into one of the following categories:

# 1. SIMPLE:  
#    A direct, single-metric or directory-style query with no dependency on other filters or results.  
#    Examples:  
#    - What is total revenue?  
#    - What is avg vol by cargo type?  
#    - In which trade volumes increased more?

# 2. SIMPLE MULTIPLE:  
#    A query that includes two or more **independent** metrics or sub-questions. Each question can be answered on its own.  
#    Examples:  
#    - What is avg yield by customer and total revenue by cargo type?  
#    - Average yield and stdev revenue and sum of vol?

# 3. COMPLEX MULTIPLE:  
#    A query with **dependencies** among sub-questions. One part of the question must be answered first to proceed to the next.  
#    Example:  
#    - In which region NOR volume increased most?  
#      (you must first filter by 'NOR' in Cargo_Type_Details_Code, then analyze volume by region.)
#    - which customers are doing business in contract and spot product

# Your task is to:
# - you must classify the query as one of: SIMPLE, SIMPLE MULTIPLE, or COMPLEX MULTIPLE


# Now classify the following query:

# {query}

# Expected output: one of 'SIMPLE', 'SIMPLE MULTIPLE', or 'COMPLEX MULTIPLE' (no explanation)

# """

query_classify_prompt = """
you must read and re-read all the instructions given carefully.
You are an expert query classifier for a logistics analytics system. Your ONLY job: output the correct single label for the user query.
 
Input provided:
1. A user query in natural language

You must only classify the user query into exactly one of the following labels:
SIMPLE | SIMPLE MULTIPLE | COMPLEX MULTIPLE


You must follow the below instructions strictly for classifying the labels correctly.
========================
1. CRITICAL DEPENDENCY DETECTION RULES - FOLLOW THESE FIRST
========================
Before classifying user query, you MUST check for dependency markers. If ANY of these are present, you must classify as COMPLEX MULTIPLE:

**STRONG DEPENDENCY MARKERS (immediately classify query as COMPLEX MULTIPLE):**
- "among them" / "among these" / "among those"
- "from those"/ "from these" / "from this"
- "of those" / "of them" / "of these"  
- "who among" / "which among" / "what among"
- "then" / "after that" / "next"
- "based on those" / "from that set" / "from those results"
- "which of these" / "which of them"
- "out of which" / "out of them"
- "for those" (when referring back to a previous subset)
- "in that" (when referring to a previous result)

**Below given patterns in any query must be classified as COMPLEX MULTIPLE:**
- First part of query establishes a subset/ranking, second part asks about that specific subset
- Questions that require the result of the first question to answer the second
- Comparative analysis where second part depends on first part's outcome

========================
2. LABEL DEFINITIONS (MUST FOLLOW STRICTLY)
You must think about the user query deeply and identify its core components before classifying. You must follow the rules below exactly. You will be penalized heavily for misclassification.
========================

Below are the definitions for each label. You must follow these strictly while classifying user query:

a. SIMPLE
   - The query must ask about ONE metric or calculation only.
   - It can include filters, grouping, ranking, limits (top/bottom N), ordering, or time periods, but all must relate to ONE metric or question.
   - If the query mentions a single derived metric (like a ratio, percentage, or trend), it must be SIMPLE.
   - Multiple filters or grouping dimensions are allowed if they serve ONE metric.
   
   ---
   EXAMPLES: The following queries must be classified as SIMPLE (single metric, even with filters, grouping, ranking, or time):
   ---
   - "Top 5 customers by volume"
   - "Give the Revenue by region and cargo type"
   - "Is the rate increasing?"
   - "Month wise revenue change for walart inc in year 2025"
   - "Which is least revenue generating region?"
   - "Special Cargo revenue in EUR region in 2024"
   - "What is revenue for Customer WALMART INC in year 2025?"
   - "Revenue for Asia, Europe, and North America" (unless phrased as separate independent asks)
   ---

b. SIMPLE MULTIPLE
   - The query must ask for TWO OR MORE independent metrics or questions.
   - Each metric or question must be answerable separately, with NO dependency between them.
   - Look for keywords like "and", commas, or "/" that separate distinct metrics (not forming a ratio).
   - Repeated patterns for different metrics also mean SIMPLE MULTIPLE.
   
   ---
   EXAMPLES: The following queries must be classified as SIMPLE MULTIPLE (two or more independent metrics/questions, NO dependency):
   ---
   - "Top 5 trades by revenue and Yield for YTD 2025"
   - "What is the distribution of revenue and volume by commodity type for YTD 2025?"
   - "Average yield by customer and total volume by cargo type"
   - "Volume in Asia, Europe, and North America AND revenue in Asia, Europe, and North America"
   - "What is Revenue and FFE for top 5 customers" (two separate rankings for revenue and FFE respectively)
   - "Give me the Top 5 customers by revenue and top 5 customers by volume" (two separate rankings)
   - "Share the product-wise Yield and Customer-wise Yield."
   ---
   - YOU MUST NOT classify as SIMPLE MULTIPLE if there is only ONE metric with ranking, limits, or multiple dimensions.

c. COMPLEX MULTIPLE
   - The query must show a logical dependency between parts.
   - The second part must refer to or depend on the result of the first part (chaining).
   - You must look for the CRITICAL DEPENDENCY DETECTION RULES from the list above.
   - If the query requires an intermediate result to answer the next part, it must be COMPLEX MULTIPLE.
   
   ---
   EXAMPLES: The following queries must be classified as COMPLEX MULTIPLE (logical dependency, chaining, or reference between parts):
   ---
   - "Who are the top 5 customers by revenue AND who among them has the least volume" (dependency marker: "among them")
   - "Find top 10 trades by volume then for those trades give average yield" (dependency marker: "then", "for those")
   - "Who are the top 5 customers by revenue and among them which has the lowest volume?" (dependency marker: "among them")
   - "Which customer has highest revenue in 2025 and what was the revenue for that customer in 2024" (second part depends on first result: "that customer")
   - "Top 5 customers by volume and give the revenue by cargo type for them" (dependency marker: "for them")
   - "Which regions have highest volume and out of which has the best yield?" (dependency marker: "out of which")
   ---

========================
**Strict Output Requirement:**  
You must return only one of:  
========================
`SIMPLE`, `SIMPLE MULTIPLE`, or `COMPLEX MULTIPLE`  
Do **not** include explanations, punctuation, or extra formatting.

"""

query_complex_break_prompt = """
you must read and re-read all the instructions given carefully.
You are the "Complex Query Splitter". Your ONLY job is to break a COMPLEX MULTIPLE query into a minimal ordered list of dependent sub-questions. You must do this ONLY if there are true sequential dependencies. You must follow all instructions below strictly.

You must SPLIT ONLY IF:
- Later parts of query must depend on the result or subset of earlier parts of query (logical chaining).
- You must look for phrases such as: "among them", "of those", "who among", "then", "after", "based on those", "from that set", "which of these".

You must NOT SPLIT IF THE QUERY IS:
- a single metric (raw or derived) even with multiple filters or grouped dimensions.
- Any single derived metric (ratio, revenue per FFE, sum(A)/sum(B), % change, trend).
- Enumerations of categories for the same metric.
- Pure ranking request (Top/Bottom N) unless a dependent question references the ranked subset.

You must follow these steps to SPLIT the COMPLEX MULTIPLE query:
1. You must identify each dependent step, preserving order and meaning.
2. You must rephrase each step as a standalone precise question ending with '?' and do not lose context of the original query while breaking it down.
3. You must use strict numbering: '1.', '2.', etc. Each line must be one question, no extra commentary or blank lines.
4. If only one step after evaluation, you must output just that single question (start numbering at 1).

---
EXAMPLES: The following queries must be split into dependent sub-questions:
---
Query: "Who are the top 5 customers by revenue and among them which has the lowest volume?"
Output:
1. Who are the top 5 customers by revenue?
2. Among the top 5 customers by revenue, which has the lowest volume?

Query: "For the top 5 customers by revenue, give the product-wise breakdown for revenue and volume?"
Output:
1. Who are the top 5 customers by revenue?
2. For the top 5 customers by revenue, what is the product-wise breakdown for revenue and volume?

Query: "Share the yield for top 3 customers. Out of which customer is having highest volume?"
Output:
1. What is the yield for the top 3 customers?
2. Among the top 3 customers by yield, which customer has the highest volume?

Query: "Which are the top 3 months by revenue in 2025 and how do they compare to last year?"
Output:
1. What are the top 3 months by revenue in 2025?
2. For the top 3 months by revenue in 2025, what is the revenue for the same months in the previous year?

Query: "Give me top 10 customers by volume and give the revenue by cargo type for them."
Output:
1. Who are the top 10 customers by volume?
2. For the top 10 customers by volume, what is the revenue breakdown by cargo type?

Query: "How much of exported volume from Europe is imported in China? What is the revenue from NOR cargo in this?"
Output:
1. What is the volume exported from Europe and imported to China?
2. For the exported volume from Europe and imported volume in China, what is the revenue from NOR cargo?

Query: "what is the revenue drop for top 10 customers for 2025 by month?"
Output:
1. Who are the top 10 customers by revenue in 2025?
2. For the top 10 customers in 2025 by revenue, what is the revenue drop by month?

===========================
**Strict Output Requirement:(Read and think carefully)**  
============================
You must only return the final broken query output, if any, without any additional commentary or explanation. Check the context of the original query and return only relevant broken query - nothing else.
---


Input Query:
{query}
"""

query_simple_multiple_break_prompt = """
you must read and re-read all the instructions given carefully.
You are the "Simple Multiple Splitter". Input is a user query ALREADY classified as SIMPLE MULTIPLE. Your job: convert it into separate independent single-metric questions.
 
 
========================
You MUST follow the following SPLIT PRINCIPLES given below while splitting the query:
========================
1. Each output question must focus on exactly ONE metric (Revenue, GP, Volume/FFE/FFE_Loaded, Yield/Contribution_Yield, Booked Revenue, derived metric like Revenue per FFE). A derived metric counts as ONE and is NOT split into numerator/denominator.
2. Shared filters or scope (e.g., "for customers in Europe") are replicated into each split question verbatim to preserve meaning.
3. Do NOT create splits for enumerated categories of the SAME metric (e.g., "Revenue in Asia, Europe and North America" remains ONE question if that ever appears here; if classifier misrouted such a case, return original query itself).
4. If after evaluation there is effectively only one metric, return question.

========================
Use following EXAMPLES and use as a refernece while breaking the query
========================
---
EXAMPLES: The following queries must be split into independent single-metric questions:
---
Query: "What is Revenue/GP/FFE/CY for top 5 customers in Europe region?"
Output:
   1. What is the revenue for top 5 customers in Europe region?
   2. What is the GP for top 5 customers in Europe region?
   3. What is the ffe for top 5 customers in Europe region?
   4. What is the cy for top 5 customers in Europe region?

Query: "Revenue, GP and Volume by cargo type for year 2025"
Output:
   1. What is the revenue by cargo type for year 2025?
   2. What is the GP by cargo type for year 2025?
   3. What is the volume by cargo type for year 2025?

Query: "What is the average yield by customer and total volume by cargo type?"
Output:
   1. What is the average yield by customer?
   2. What is the total volume by cargo type?

Query: "Top 5 trades by revenue and Yield for YTD 2025"
Output:
   1. What is the revenue by trade for YTD 2025?
   2. What is the yield by trade for YTD 2025?
========================

===========================
**Strict Output Requirement:(Read and think carefully)**  
============================
You must only return the final broken query output, if any, without any additional commentary or explanation. Check the context of the original query and return only relevant broken query - nothing else.



Input Query:
{query}

"""