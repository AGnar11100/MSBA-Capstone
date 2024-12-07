from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, expr, split
from pyspark.sql.types import IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName('EV Data Processing').getOrCreate()

# Base GCS paths
RAW_PATH_PREFIX = "gs://raw-data-bucket-msba-ev-wa/"
TRANSFORMED_PATH_PREFIX = "gs://transformed-data-bucket-msba-ev-wa/"

# Define paths for CSVs
csv_files = {
    'ev_population_data': 'Electric_Vehicle_Population_Data_20240920.csv',
    'zip_county_data': 'ZIP-COUNTY-FIPS_2017-06.csv',
    'ev_population_history': 'Electric_Vehicle_Population_Size_History.csv',
    'station_locations': 'alt_fuel_stations (Sep 21 2024).csv',
    'power_plants': 'Power_Plants.csv'
}

# Reading CSVs from GCS
dataframes = {}
for name, filename in csv_files.items():
    path = RAW_PATH_PREFIX + filename
    df = spark.read.csv(path, header=True, inferSchema=True)
    for column in df.columns:
        if df.schema[column].dataType == StringType():
            df = df.withColumn(column, regexp_replace(col(column), '"', '""'))  # Escaping quotes
    dataframes[name] = df
    dataframes[name] = df
    print(f"Columns in {name}: {df.columns}")

# Access dataframes by key
ev_pop_df = dataframes['ev_population_data']
zipcounty_df = dataframes['zip_county_data']
sizehist_df = dataframes['ev_population_history']
stationlocs_df = dataframes['station_locations']
powerplants_df = dataframes['power_plants']

# Transforming zipcounty_df
zipcounty_df = zipcounty_df.select(
    col("ZIP").alias("ZIP"),
    col("COUNTYNAME").alias("County"),
    col("STCOUNTYFP").alias("Area_Code")
)

# WA EV Population DataFrame Transformation
# WA EV Population DataFrame Transformation
ev_pop_df = dataframes['ev_population_data'].select(
    col("County"),
    col("City"),
    col("State"),
    col("Postal Code").alias("ZIP"),
    col("Model Year").alias("Model_Year"),
    col("Make"),
    col("Model"),
    col("Electric Vehicle Type").alias("Electric_Vehicle_Type"),
    col("Clean Alternative Fuel Vehicle (CAFV) Eligibility").alias("CAFV_Eligibility"),
    col("Electric Range").alias("Electric_Range"),
    col("Legislative District").alias("Legislative_District"),
    col("DOL Vehicle ID").alias("DOL_Vehicle_ID"),
    col("Electric Utility").alias("Electric_Utility"),
    col("2020 Census Tract").alias("Census_Tract"),
    expr("split(substring(trim(substring_index(`Vehicle Location`, '(', -1)), 1, length(trim(substring_index(`Vehicle Location`, '(', -1))) - 1), ' ')[0]").cast('float').alias('Longitude'),
    expr("split(substring(trim(substring_index(`Vehicle Location`, '(', -1)), 1, length(trim(substring_index(`Vehicle Location`, '(', -1))) - 1), ' ')[1]").cast('float').alias('Latitude')
)

# WA EV History DataFrame Transformation
history_df = dataframes['ev_population_history'].select(
    col("Date").alias("Date"),
    col("Plug-In Hybrid Electric Vehicle (PHEV) Count").alias("PHEV_Count"),
    col("Battery Electric Vehicle (BEV) Count").alias("BEV_Count"),
    col("Electric Vehicle (EV) Total").alias("EV_Total")
)

# Charging Stations DataFrame Transformation
stations_df = dataframes['station_locations'].select(
    col("Fuel Type Code").alias("Fuel_Type_Code"),
    col("Station Name").alias("Station_Name"),
    col("State").alias("State"),
    col("ZIP").alias("ZIP"),
    col("Status Code").alias("Status_Code"),
    col("Groups With Access Code").alias("Groups_With_Access_Code"),
    col("Access Days Time").alias("Access_Days_Time"),
    col("EV Network").alias("EV_Network"),
    col("Geocode Status").alias("Geocode_Status"),
    col("Latitude").alias("Latitude"),
    col("Longitude").alias("Longitude"),
    col("Date Last Confirmed").alias("Date_Last_Confirmed"),
    col("ID").alias("ID"),
    col("Updated At").alias("Updated_At"),
    col("Open Date").alias("Open_Date"),
    col("EV Connector Types").alias("EV_Connector_Types"),
    col("Country").alias("Country"),
    col("Groups With Access Code (French)").alias("Groups_With_Access_Code_French"),
    col("Access Code").alias("Access_Code"),
    col("EV Workplace Charging").alias("EV_Workplace_Charging")
)

# Power Plants DataFrame Transformation
power_plants_df = dataframes['power_plants'].select(
    col("OBJECTID").alias("OBJECTID"),
    col("Plant_Code").alias("Plant_Code"),
    col("Plant_Name").alias("Plant_Name"),
    col("Utility_ID").alias("Utility_ID"),
    col("Utility_Name").alias("Utility_Name"),
    col("sector_name").alias("sector_name"),
    col("City").alias("City"),
    col("County").alias("County"),
    col("State").alias("State"),
    col("Zip").alias("ZIP"),
    col("PrimSource").alias("PrimSource"),
    col("source_desc").alias("source_desc"),
    col("tech_desc").alias("tech_desc"),
    col("Install_MW").alias("Install_MW"),
    col("Total_MW").alias("Total_MW"),
    col("Longitude").alias("Longitude"),
    col("Latitude").alias("Latitude")
)

# Writing transformed DataFrames back to GCS
output_files = {
    'Zips_Counties': zipcounty_df,
    'Charging_Stations': stations_df,
    'Power_Plants': power_plants_df,
    'WA_EV_Population': ev_pop_df,
    'WA_EV_History': history_df
}

for key, df in output_files.items():
    file_path = f"{TRANSFORMED_PATH_PREFIX}transformed_data/{key.replace(' ', '_').lower()}.csv"
    df.write.mode('overwrite').csv(file_path, header=True)

# Stop Spark session
spark.stop()
