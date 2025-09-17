"""
Data dictionary containing column names and descriptions for the dataset.
This file can be modified without affecting the query correction logic.
"""

# Dictionary mapping column names to their descriptions
DATA_DICTIONARY = {
    "Cargo_Type_Name": "It is the Cargo Type",
    "Cargo_Type_Details_Code": "It is details of Cargo type",
    "Commodity_Sub_Type_Code": "It is the Commodity types",
    "Contract_Product_Lvl1": "It is the level 1 Contract Product type",
    "Contract_Product_Lvl2": "It is the level 2 Contract Product type",
    "Contract_Product_Segment": "It is the Segment of Contract Products",
    "Contract_Length": "It is the length of the contract based on month",
    "Contractual_Customer_Code": "It is the Code of Contractual Customer (Default For Customer)",
    "Contractual_Customer_Name": "It is the Name of Contractual Customer (Default For Customer)",
    "Contractual_Customer_Country_Name": "It is the Country name of Contractual Customer (Default For Customer)",
    "Contractual_Customer_Area_Name": "It is the Area Name of Contractual Customer (Default For Customer)",
    "Contractual_Customer_Region_Name": "It is the Region Name of Contractual Customer (Default For Customer)",
    "Contractual_Customer_Concern_Code": "It is the Concern Code of Contractual Customer",
    "Contractual_Customer_Concern_Name": "It is the Concern Name of Contractual Customer",
    "Contractual_Customer_Consolidated_Code": "It is the Consolidated Code of Contractual Customer",
    "Contractual_Customer_Consolidated_Name": "It is the Consolidated Name of Contractual Customer",
    "Contractual_Customer_Value_Proposition": "It is the Value Proposition of Contractual Customer",
    "Contractual_Customer_Vertical": "It is the Vertical of Contractual Customer",
    "Week": "It is the Year and week of First load Date (%Y- Weeknumber)",
    "Month_Unique": "It is the Month and Year of First load Date (%Y-%m-%d)",
    "Quarter_Unique": "It is the Quarter and Year of the First Load Date (Quarter - %Y)",
    "Year": "It is the Year of First load Date (YYYY)",
    "Equipment_SubType_Code": "It is the Code for Equipment sub type",
    "Equipment_Type_Code": "It is the code for Equipment Type",
    "Equipment_SubSize_Code": "It is the Code for Equipment Sub size",
    "Equipment_Height_Code": "It is the code for Equipment Height",
    "Geo_Site_Code_POR": "It is the Site code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)",
    "Geo_Site_Name_POR": "It is the Site name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)",
    "Geo_City_Code_POR": "It is the City Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)",
    "Geo_City_Name_POR": "It is the City Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)",
    "Geo_Country_Code_POR": "It is the Country Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)",
    "Geo_Country_Name_POR": "It is the Country Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)",
    "Geo_Area_Code_POR": "It is the Area Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)",
    "Geo_Area_Name_POR": "It is the Area Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)",
    "Geo_Region_Code_POR": "It is the Region Code for Place of Receipt (POR) (For Export and Default if Nothing mentioned)",
    "Geo_Region_Name_POR": "It is the Region Name for Place of Receipt (POR) (For Export and Default if Nothing mentioned)",
    "Geo_Site_Code_POD": "It is the Site code for Place of Delivery (POD) (For Import)",
    "Geo_Site_Name_POD": "It is the Site name for Place of Delivery (POD) (For Import)",
    "Geo_City_Code_POD": "It is the City Code for Place of Delivery (POD) (For Import)",
    "Geo_City_Name_POD": "It is the City Name for Place of Delivery (POD) (For Import)",
    "Geo_Country_Code_POD": "It is the Country Code for Place of Delivery (POD) (For Import)",
    "Geo_Country_Name_POD": "It is the Country Name for Place of Delivery (POD) (For Import)",
    "Geo_Area_Code_POD": "It is the Area Code for Place of Delivery (POD) (For Import)",
    "Geo_Area_Name_POD": "It is the Area Name for Place of Delivery (POD) (For Import)",
    "Geo_Region_Code_POD": "It is the Region Code for Place of Delivery (POD) (For Import)",
    "Geo_Region_Name_POD": "It is the Region Name for Place of Delivery (POD) (For Import)",
    "Geo_Site_Code_LOPFI": "It is the Site code for First Load Port (LOPFI or FLP)",
    "Geo_Site_Name_LOPFI": "It is the Site name for First Load Port (LOPFI or FLP)",
    "Geo_City_Code_LOPFI": "It is the City Code for First Load Port (LOPFI or FLP)",
    "Geo_City_Name_LOPFI": "It is the City Name for First Load Port (LOPFI or FLP)",
    "Geo_Country_Code_LOPFI": "It is the Country Code for First Load Port (LOPFI or FLP)",
    "Geo_Country_Name_LOPFI": "It is the Country Name for First Load Port (LOPFI or FLP)",
    "Geo_Area_Code_LOPFI": "It is the Area Code for First Load Port (LOPFI or FLP)",
    "Geo_Area_Name_LOPFI": "It is the Area Name for First Load Port (LOPFI or FLP)",
    "Geo_Region_Code_LOPFI": "It is the Region Code for First Load Port (LOPFI or FLP)",
    "Geo_Region_Name_LOPFI": "It is the Region Name for First Load Port (LOPFI or FLP)",
    "Geo_Site_Code_DIPLA": "It is the Site code for Last Discharge Port (DIPLA or LDP)",
    "Geo_Site_Name_DIPLA": "It is the Site name for Last Discharge Port (DIPLA or LDP)",
    "Geo_City_Code_DIPLA": "It is the City Code for Last Discharge Port (DIPLA or LDP)",
    "Geo_City_Name_DIPLA": "It is the City Name for Last Discharge Port (DIPLA or LDP)",
    "Geo_Country_Code_DIPLA": "It is the Country Code for Last Discharge Port (DIPLA or LDP)",
    "Geo_Country_Name_DIPLA": "It is the Country Name for Last Discharge Port (DIPLA or LDP)",
    "Geo_Area_Code_DIPLA": "It is the Area Code for Last Discharge Port (DIPLA or LDP)",
    "Geo_Area_Name_DIPLA": "It is the Area Name for Last Discharge Port (DIPLA or LDP)",
    "Geo_Region_Code_DIPLA": "It is the Region Code for Last Discharge Port (DIPLA or LDP)",
    "Geo_Region_Name_DIPLA": "It is the Region Name for Last Discharge Port (DIPLA or LDP)",
    "Geo_Country_Code_Sales_Control": "It is the Country Code for Sales Control",
    "Geo_Country_Name_Sales_Control": "It is the Country Name for Sales Control",
    "Geo_Area_Code_Sales_Control": "It is the Area Code for Sales Control",
    "Geo_Area_Name_Sales_Control": "It is the Area Name for Sales Control",
    "Geo_Region_Code_Sales_Control": "It is the Region Code for Sales Control",
    "Geo_Region_Name_Sales_Control": "It is the Region Name for Sales Control",
    "Container_Ownership_Code": "It is the Code for Container Ownership (Owned by Maersk or Competitors)",
    "Rate_Length": "It is the Length of the contracts basis rate exposure (Short Term and Long Term)",
    "Rate_Length_lvl1": "It is the Level 1 Details for Rate length (Also known as Modelship or rate validity)",
    "Rate_Mechanism": "It is the Rate Mechanism Details",
    "Rate_Review_Period": "It is the Review Period for Rate lengths",
    "String": "It is the String field containing Trade related information",
    "Trade_Code": "It is the Trade lane code",
    "Trade": "It is the Trade lane",
    "Trade_Cluster": "It is the Trade lane Cluster",
    "Trade_Segment": "It is the segment for Trade lane",
    "Profit_Center": "It is the Profit Centre for Trade lane",
    "String_Direction_Code": "It is the code for direction of String or Trade",
    "String_Direction_Name": "It is the name of direction of String or Trade",
    "String_Flow_Direction_Name": "It is the name of the Direction flow for String or Trade",
    "Trade_Market": "it is the Trade Market details and Direction flow",
    "Break_Bulk_Flag": "It is the flag for goods transported individually, rather than in containers like big machines",
    "Value_Protect_Flag": "It is a flag to indicate the Insurance for Cargo",
    "OOG_Flag": "It is the Flag to identify Out of Gauge (when cargo is too large to fit inside standard shipping containers)",
    "AllocatedAmountUSD": "It is the Revenue",
    "FFE_Loaded": "It is the Volume or FFE",
    "Booked_FFE": "It is the Booked Volume or FFE",
    "BookedRevenue_USD": "It is the Booked Revenue",
    "Contribution_Yield": "It is the Contribution Yield or GP",
    "Activity_source_cd": "It is the Activity Source Code"
    #  Add all your columns here
}

# Optional: Sample values for each column to enhance query correction
SAMPLE_DATA = {
    "Cargo_Type_Name": ["Dry", "Reefer", "Unknown"],
    "Cargo_Type_Details_Code": ["DRY", "REEFER", "SPECIAL", "NOR"],
    "Commodity_Sub_Type_Code": ["Appliances and kitchenware", "Textiles and apparel", "Chemicals", "Plastic and rubber", "Metal", "Furniture", "Vehicles", "Foodstuff", "Miscellaneous manufactured materials", "Paper"],
    "Contract_Product_Lvl1": ["Offline", "Spot", "Contract Partner Products", "Maersk Go"],
    "Contract_Product_Lvl2": ["Offline", "Spot", "Block Space", "Maersk Go", "Flexible", "Reef"],
    "Contract_Product": ["UNK", "Maersk Spot", "Block space plus", "Maersk Spot Rollable", "Sealand Spot", "Maersk Go", "Flex Standard LAB", "Cold Chain Logistics", "Sealand SpotRollable", "Flex Essential LAB"],
    "Contract_Product_Segment": ["Contracts", "Shipments"],
    "Contract_Length": ["Long Term", "Short term less than 1 month", "Short term more than 1 month", "Unknown"],
    "Contractual_Customer_Area_Name": ["Greater China Area", "North America Area", "North Europe Continent Area", "India and Bangladesh Area", "East Coast South America Area", "Eastern Mediterranean Area", "South West Europe Area", "Nordic Area", "South-East Asia Area", "North East Asia Area"],
    "Contractual_Customer_Region_Name": ["Asia Pacific Region", "Europe Region", "North America Region", "India, Middle East, and Africa", "Latin America Region", "Unknown"],
    "Contractual_Customer_Value_Proposition": ["EFFICIENCY", "EXPERTISE", "AMBITION", "EASE", "Unknown", "SUPER SAVER", "ADVANCEMENT"],
    "Contractual_Customer_Vertical": ["TRANSPORT FREIGHT & STORAGE", "RETAIL", "REEFER", "FMCG FOOD AND BEVERAGE", "CHEMICALS", "LIFESTYLE", "OTHER", "TECHNOLOGY", "AUTOMOTIVE", "Unknown"],
    "Month_Unique":['2023-04-01', '2024-12-01', '2024-06-01', '2024-01-01', '2024-02-01', '2024-03-01', '2025-01-01', '2025-02-01', '2025-03-01', '2025-05-01', '2025-06-01', '2025-07-01'],
    "Quarter_Unique": ["Q3 - 2023", "Q4 - 2023", "Q3 - 2024", "Q4 - 2024", "Q2 - 2023", "Q2 - 2024", "Q1 - 2024", "Q1 - 2025", "Q1 - 2023", "Q2 - 2025"],
    "Quarter_Code_YYYYQQ": [202303, 202304, 202403, 202404, 202302, 202402, 202401, 202501, 202301, 202502],
    "Year": [2023, 2024, 2025],
    "Equipment_SubType_Code": ["DRY", "REEF", "-1", "TANK", "OPEN", "FLAT", "PLWD"],
    "Equipment_Type_Code": ["DRY", "REEF", "-1"],
    "Equipment_SubSize_Code": [40.0, 20.0, -1.0, 45.0],
    "Equipment_Height_Code": ["9.6", "8.6", "-1"],
    "Geo_Area_Code_POR": ["GCA", "IBS", "NOA", "NEC", "ESA", "MEK", "EME", "SEA", "SWE", "CSE"],
    "Geo_Area_Name_POR": ["Greater China Area", "India and Bangladesh Area", "North America Area", "North Europe Continent Area", "East Coast South America Area", "Mekong Area", "Eastern Mediterranean Area", "South-East Asia Area", "South West Europe Area", "Central South Europe Area"],
    "Geo_Region_Code_POR": ["APA", "EUR", "IMEA", "LAM", "NAM", "-1"],
    "Geo_Region_Name_POR": ["Asia Pacific Region", "Europe Region", "India, Middle East, and Africa", "Latin America Region", "North America Region", "Unknown"],
    "Geo_Area_Code_POD": ["NOA", "NEC", "ESA", "WAF", "CAC", "SEA", "EME", "SWE", "OCE", "IBS"],
    "Geo_Area_Name_POD": ["North America Area", "North Europe Continent Area", "East Coast South America Area", "West Africa Area", "Central America, Andina, Caribbean", "South-East Asia Area", "Eastern Mediterranean Area", "South West Europe Area", "Oceania Area", "India and Bangladesh Area"],
    "Geo_Region_Code_POD": ["EUR", "IMEA", "APA", "NAM", "LAM", "-1"],
    "Geo_Region_Name_POD": ["Europe Region", "India, Middle East, and Africa", "Asia Pacific Region", "North America Region", "Latin America Region", "Unknown"],
    "Geo_Area_Code_LOPFI": ["GCA", "IBS", "NEC", "NOA", "ESA", "MEK", "EME", "SEA", "SWE", "CSE"],
    "Geo_Area_Name_LOPFI": ["Greater China Area", "India and Bangladesh Area", "North Europe Continent Area", "North America Area", "East Coast South America Area", "Mekong Area", "Eastern Mediterranean Area", "South-East Asia Area", "South West Europe Area", "Central South Europe Area"],
    "Geo_Region_Code_LOPFI": ["APA", "EUR", "IMEA", "LAM", "NAM", "-1"],
    "Geo_Region_Name_LOPFI": ["Asia Pacific Region", "Europe Region", "India, Middle East, and Africa", "Latin America Region", "North America Region", "Unknown"],
    "Geo_Area_Code_DIPLA": ["NOA", "NEC", "ESA", "WAF", "CAC", "SEA", "EME", "OCE", "SWE", "IBS"],
    "Geo_Area_Name_DIPLA": ["North America Area", "North Europe Continent Area", "East Coast South America Area", "West Africa Area", "Central America, Andina, Caribbean", "South-East Asia Area", "Eastern Mediterranean Area", "Oceania Area", "South West Europe Area", "India and Bangladesh Area"],
    "Geo_Region_Code_DIPLA": ["EUR", "IMEA", "APA", "NAM", "LAM", "-1"],
    "Geo_Region_Name_DIPLA": ["Europe Region", "India, Middle East, and Africa", "Asia Pacific Region", "North America Region", "Latin America Region", "Unknown"],
    "Geo_Area_Code_Sales_Control": ["GCA", "NOA", "NEC", "NDC", "IBS", "ESA", "SWE", "EME", "NEA", "CSE"],
    "Geo_Area_Name_Sales_Control": ["Greater China Area", "North America Area", "North Europe Continent Area", "Nordic Area", "India and Bangladesh Area", "East Coast South America Area", "South West Europe Area", "Eastern Mediterranean Area", "North East Asia Area", "Central South Europe Area"],
    "Geo_Region_Code_Sales_Control": ["EUR", "APA", "NAM", "IMEA", "LAM", "-1"],
    "Geo_Region_Name_Sales_Control": ["Europe Region", "Asia Pacific Region", "North America Region", "India, Middle East, and Africa", "Latin America Region", "Unknown"],
    "Container_Ownership_Code": ["COC", "SOC", "-1", "MLO"],
    "Rate_Length": ["Short Term", "Long Term"],
    "Rate_Length_lvl1": ["Long Term Rate Contracts", "Online Solutions", "Short Term Rate Contracts"],
    "Rate_Mechanism": ["Contract Length >3M", "Online Rate", "Contract Length <1M", "Block Space", "Contract Length >1M"],
    "Rate_Review_Period": ["Below 1 Month", "Above 3 Months", "Below 3 Months"],
    "Trade_Code": ["E1", "E2", "C1", "P1", "P3", "E4", "B1", "E3", "W1", "M3"],
    "Trade": ["E1 - Far East - North Europe", "E2 - Far East - Mediterranean", "C1 - Asia - CAM/CAR", "P1 - Pacific West Coast", "P3 - Pacific East Coast", "E4 - WCA - Mediterranean", "B1 - WCSA - Europe", "E3 - Middle East - Europe", "W1 - Far East - West Africa", "M3 - WCA - North America"],
    "Trade_Cluster": ["TSU - Trans-Suez", "NAM - North America", "LAM - Latin America", "AFR - Africa", "IAS - Intra Asia", "OCE - Oceania", "IAM - Intra Americas", "IEG - Intra Europe & Gulf", "BRC - Brazil Cabotage", "-1 - Unknown"],
    "Trade_Segment": ["LH - Long Hauls", "SH - Short Hauls"],
    "Profit_Center": ["AEN - Asia - NEU", "WCSA - West Coast South America", "IAS - Intra Asia", "PAC - Transpacific", "AEM - Asia - MED", "MEE - Middle East - Europe", "WAF - West Africa", "OCE - Oceania", "ATL - Atlantic incl MECL", "ECSA - East Coast South America"],
    "String_Direction_Code": ["W", "E", "S", "R", "N", "FW", "FS", "ES", "MW", "SE"],
    "String_Direction_Name": ["Westbound", "Eastbound", "Southbound", "Wayport", "Northbound", "Far East / West Africa", "Far East - South America", "Europe - South America", "Middle East-I.P./West Africa", "South America - Europa"],
    "String_Flow_Direction_Name": ["Headhaul", "Backhaul", "Unknown"],
    "Trade_Market": ["E-W - East-West", "N-S - North-South", "I-R - Intra-Regional"],
    "Break_Bulk_Flag": ["N", "Y"],
    "Value_Protect_Flag": ["N", "Y"],
    "OOG_Flag": ["N", "Y"],
}


APP_FILTERS = {
"Container_Ownership_Code": ["COC", "SOC", "-1", "MLO"],
"Geo_Area_Code_POR":  ["NOA", "GCA", "UAE", "PAK", "ESA", "EME", "MEK", "NEC", "WAF", "WSA", "IBS", "NDC", "SWE", "SAA", "SEA", "OCE", "UKI", "SAI", "EAA", "CSE", "CAC", "NEA", "-1","USA","MXA"],
"Geo_Region_Code_POR": ["APA", "EUR", "IMEA", "LAM", "NAM", "-1"],
"Activity Source Code": ["ETD", "ATD", "Unknown"],
"String_Flow_Direction_Name": ["Backhaul", "Headhaul", "Unknown"]
}




def get_data_dictionary():
    """Return the data dictionary"""
    return DATA_DICTIONARY

def get_sample_data():
    """Return sample data values"""
    return SAMPLE_DATA

def add_column(column_name, description):
    """
    Add a new column to the data dictionary
    
    Args:
        column_name (str): Name of the column
        description (str): Description of the column
    """
    DATA_DICTIONARY[column_name] = description
    
def remove_column(column_name):
    """
    Remove a column from the data dictionary
    
    Args:
        column_name (str): Name of the column to remove
    
    Returns:
        bool: True if column was removed, False if it didn't exist
    """
    if column_name in DATA_DICTIONARY:
        del DATA_DICTIONARY[column_name]
        return True
    return False

def add_sample_data(column_name, values):
    """
    Add sample values for a column
    
    Args:
        column_name (str): Name of the column
        values (list): List of sample values
    """
    SAMPLE_DATA[column_name] = values