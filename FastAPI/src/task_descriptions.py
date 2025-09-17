tasks = {
"performance": """
Performance of a key is change in revenue. 
If nothing is mentioned then calculate sum of revenue year wise and show.
If month, year or week is mentioned then do as it has been mentioned. 
""",

"rate":"""
rate is Revenue per FFE.
If contract rate is mentioned then consider "Contracts" which is present in Contract_Product_Segment.
""",

"Revenue/FFE" : """
Revenue per FFE is calculated as AllocatedAmountUSD divided by FFE_Loaded.
If Modelship (Rate_Length_lvl1) is mentioned, group by Rate_Length_lvl1.
If no grouping is specified, calculate the overall revenue per FFE.
""",

"MoM": """
Month on Month (MoM) change is calculated as the difference between the current month and the previous month.
By default, calculate MoM change using .diff(), which provides the absolute difference between the current and previous month.
You must use .pct_change() if the query explicitly asks for MoM with percentage change otherwise you must use .diff() only.
""",

"YoY": """
Year on Year (YoY) change is calculated as the difference between the current year and the previous year.
By default, calculate YoY change using .diff(), which provides the absolute difference between the current and previous year.
You must use .pct_change() if the query explicitly asks for YoY with percentage change otherwise you must use .diff() only.
"""
}