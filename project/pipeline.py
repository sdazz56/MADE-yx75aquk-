import os
import pandas as pd
import sqlite3
import requests
import webbrowser

# URLs for the datasets
url1 = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sdg_13_10?format=TSV&compressed=false"
url2 = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sdg_13_50?format=TSV&compressed=false"
url3 = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ten00124?format=TSV&compressed=false"

# Data directory
output_dir = "output"
csv_paths = {
    "data1": os.path.join(output_dir, "final_energy_consumption_by_sector.csv"),
    "data2": os.path.join(output_dir, "net_greenhouse_gas_emissions.csv"),
    "data3": os.path.join(output_dir, "climate_related_expenditure.csv")
}
database_paths = {
    "database1": os.path.join(output_dir, "final_energy_consumption_by_sector.db"),
    "database2": os.path.join(output_dir, "net_greenhouse_gas_emissions.db"),
    "database3": os.path.join(output_dir, "climate_related_expenditure.db")
}

# Create output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Function to download and save files
def download_file(url, file_path):
    if not os.path.exists(file_path):
        response = requests.get(url)
        response.raise_for_status()
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {file_path}")
    else:
        print(f"File {file_path} already exists. Skipping download.")

# Download the datasets
print("Downloading datasets...")
download_file(url1, csv_paths["data1"])
download_file(url2, csv_paths["data2"])
download_file(url3, csv_paths["data3"])
print("Download complete.")

# Read the datasets into DataFrames
print("Reading datasets into DataFrames...")
df1 = pd.read_csv(csv_paths["data1"], delimiter='\t')
df2 = pd.read_csv(csv_paths["data2"], delimiter='\t')
df3 = pd.read_csv(csv_paths["data3"], delimiter='\t')

# Fill missing values with 0
print("Filling missing values...")
df1.fillna(0, inplace=True)
df2.fillna(0, inplace=True)
df3.fillna(0, inplace=True)

# Clean column names (strip and lowercase)
print("Cleaning column names...")
df1.columns = [col.strip().lower() for col in df1.columns]
df2.columns = [col.strip().lower() for col in df2.columns]
df3.columns = [col.strip().lower() for col in df3.columns]

# Save cleaned DataFrames to CSV files
def save_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)
    print(f"Data saved to {csv_path}")

# Save cleaned DataFrames as CSV files
save_to_csv(df1, csv_paths["data1"])
save_to_csv(df2, csv_paths["data2"])
save_to_csv(df3, csv_paths["data3"])

print("Data cleaning and saving completed.")

# Save DataFrames to SQLite databases
def save_to_sqlite(df, db_path, table_name):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"Data saved to {db_path} in table {table_name}")

save_to_sqlite(df1, database_paths["database1"], "final_energy_consumption_by_sector")
save_to_sqlite(df2, database_paths["database2"], "net_greenhouse_gas_emissions")
save_to_sqlite(df3, database_paths["database3"], "climate_related_expenditure")

print("Data pipeline execution completed.")

# Open files with specified names
def open_files(file_paths):
    for file_path, display_name in file_paths.items():
        if os.path.exists(file_path):
            webbrowser.open(file_path)
        else:
            print(f"{file_path} does not exist.")

# Open CSV files and SQLite databases with different names
csv_files = {
    csv_paths["data1"]: "Final Energy Consumption by Sector Data",
    csv_paths["data2"]: "Net Greenhouse Gas Emissions Data",
    csv_paths["data3"]: "Climate-related Expenditure Data"
}
open_files(csv_files)

sqlite_dbs = {
    database_paths["database1"]: "Final Energy Consumption by Sector Database",
    database_paths["database2"]: "Net Greenhouse Gas Emissions Database",
    database_paths["database3"]: "Climate-related Expenditure Database"
}
open_files(sqlite_dbs)

