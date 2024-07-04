import os
import pandas as pd
import sqlite3
import requests
import webbrowser

# URLs for the datasets
urls = {
    "energy_consumption_by_sector": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ten00124?format=TSV&compressed=false",
    "GHG_emissions": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sdg_13_10?format=TSV&compressed=false",
    "climate_related_expenditure": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sdg_13_50?format=TSV&compressed=false"
}

# Data directory
output_dir = "output"
tsv_paths = {
    "energy_consumption_by_sector": os.path.join(output_dir, "final_energy_consumption_by_sector.tsv"),
    "GHG_emissions": os.path.join(output_dir, "net_greenhouse_gas_emissions.tsv"),
    "climate_related_expenditure": os.path.join(output_dir, "climate_related_expenditure.tsv")
}
excel_paths = {
    "energy_consumption_by_sector": os.path.join(output_dir, "final_energy_consumption_by_sector.xlsx"),
    "GHG_emissions": os.path.join(output_dir, "net_greenhouse_gas_emissions.xlsx"),
    "climate_related_expenditure": os.path.join(output_dir, "climate_related_expenditure.xlsx")
}
database_paths = {
    "energy_consumption_by_sector": os.path.join(output_dir, "final_energy_consumption_by_sector.db"),
    "GHG_emissions": os.path.join(output_dir, "net_greenhouse_gas_emissions.db"),
    "climate_related_expenditure": os.path.join(output_dir, "climate_related_expenditure.db")
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
for key, url in urls.items():
    download_file(url, tsv_paths[key])
print("Download complete.")

# Read the datasets into DataFrames
print("Reading datasets into DataFrames...")
energy_consumption_by_sector = pd.read_csv(tsv_paths["energy_consumption_by_sector"], delimiter='\t', encoding='ISO-8859-1')
GHG_emissions = pd.read_csv(tsv_paths["GHG_emissions"], delimiter='\t', encoding='ISO-8859-1')
climate_related_expenditure = pd.read_csv(tsv_paths["climate_related_expenditure"], delimiter='\t', encoding='ISO-8859-1')

# Fill missing values with 0
print("Filling missing values...")
energy_consumption_by_sector.fillna(0, inplace=True)
GHG_emissions.fillna(0, inplace=True)
climate_related_expenditure.fillna(0, inplace=True)

# Clean column names (strip and lowercase)
print("Cleaning column names...")
energy_consumption_by_sector.columns = [col.strip().lower() for col in energy_consumption_by_sector.columns]
GHG_emissions.columns = [col.strip().lower() for col in GHG_emissions.columns]
climate_related_expenditure.columns = [col.strip().lower() for col in climate_related_expenditure.columns]

# Function to remove 'b' from specific columns and convert to numeric
def clean_columns(df, columns):
    for column in columns:
        df[column] = df[column].astype(str).str.replace('b', '').str.replace('p', '')
    return df

# Apply the cleaning function to the necessary columns
columns_to_clean = [str(year) for year in range(2020, 2023)]
GHG_emissions = clean_columns(GHG_emissions, columns_to_clean)

# Transpose the DataFrame
def transpose_and_set_index(df):
    df_transposed = df.transpose()
    df_transposed.columns = df_transposed.iloc[0]  # Set the first row as column headers
    df_transposed = df_transposed[1:]  # Exclude the first row as it's now the header
    df_transposed.index.name = 'year'  # Rename the index to 'year'
    df_transposed.reset_index(inplace=True)  # Reset the index to make 'year' a column
    return df_transposed

energy_consumption_by_sector_transposed = transpose_and_set_index(energy_consumption_by_sector)
GHG_emissions_transposed = transpose_and_set_index(GHG_emissions)
climate_related_expenditure_transposed = transpose_and_set_index(climate_related_expenditure)

# Filter the DataFrame to include only years from 2014 to 2022
years_range = [str(year) for year in range(2014, 2023)]
energy_consumption_by_sector_transposed = energy_consumption_by_sector_transposed[energy_consumption_by_sector_transposed['year'].isin(years_range)]
GHG_emissions_transposed = GHG_emissions_transposed[GHG_emissions_transposed['year'].isin(years_range)]
climate_related_expenditure_transposed = climate_related_expenditure_transposed[climate_related_expenditure_transposed['year'].isin(years_range)]

# Save cleaned DataFrames to Excel files
def save_to_excel(df, excel_path):
    if os.path.exists(excel_path):
        os.remove(excel_path)
    df.to_excel(excel_path, index=False)
    print(f"Data saved to {excel_path}")

# Save transposed DataFrames as Excel files
save_to_excel(energy_consumption_by_sector_transposed, excel_paths["energy_consumption_by_sector"])
save_to_excel(GHG_emissions_transposed, excel_paths["GHG_emissions"])
save_to_excel(climate_related_expenditure_transposed, excel_paths["climate_related_expenditure"])

print("Data cleaning and saving completed.")

# Save DataFrames to SQLite databases
def save_to_sqlite(df, db_path, table_name):
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"Data saved to {db_path} in table {table_name}")

save_to_sqlite(energy_consumption_by_sector_transposed, database_paths["energy_consumption_by_sector"], "final_energy_consumption_by_sector")
save_to_sqlite(GHG_emissions_transposed, database_paths["GHG_emissions"], "net_greenhouse_gas_emissions")
save_to_sqlite(climate_related_expenditure_transposed, database_paths["climate_related_expenditure"], "climate_related_expenditure")

print("Data pipeline execution completed.")

# Open files with specified names
def open_files(file_paths):
    for file_path, display_name in file_paths.items():
        if os.path.exists(file_path):
            print(f"Opening {display_name} at {file_path}")
            webbrowser.open(f'file://{os.path.abspath(file_path)}')
        else:
            print(f"{file_path} does not exist.")

# Open Excel files and SQLite databases with different names
excel_files = {
    excel_paths["energy_consumption_by_sector"]: "Final Energy Consumption by Sector Data",
    excel_paths["GHG_emissions"]: "Net Greenhouse Gas Emissions Data",
    excel_paths["climate_related_expenditure"]: "Climate Related Expenditure Data"
}
open_files(excel_files)

sqlite_dbs = {
    database_paths["energy_consumption_by_sector"]: "Final Energy Consumption by Sector Database",
    database_paths["GHG_emissions"]: "Net Greenhouse Gas Emissions Database",
    database_paths["climate_related_expenditure"]: "Climate Related Expenditure Database"
}
open_files(sqlite_dbs)
