# Project Plan

## Title
<!-- Give your project a short title. -->
## Correlation Analysis between Final Energy Consumption, Net Greenhouse Gas Emissions, and Climate-related Expenditure

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
What are the relationships between final energy consumption, net greenhouse gas emissions, and climate-related expenditure in the France?

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
This project aims to examine the connections between energy consumption, environmental impact, and financial contributions to climate-related initiatives in the France. By analyzing datasets on final energy consumption, net greenhouse gas emissions, and climate-related expenditure, we seek to uncover patterns and correlations that can provide insights into the France’s efforts towards sustainable development and climate mitigation.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->
Data Source 1: Final Energy Consumption by Sector

	•	Source: Eurostat API
	•	Metadata URL: Eurostat Metadata
	•	Data URL: Eurostat Data
	•	Data Type: TSV
	•	Description: Provides data on final energy consumption by sector, allowing analysis of energy usage patterns.

Data Source 2: Net Greenhouse Gas Emissions

	•	Source: Eurostat API
	•	Metadata URL: Eurostat Metadata
	•	Data URL: Eurostat Data
	•	Data Type: TSV
	•	Description: Contains information on net greenhouse gas emissions, facilitating analysis of environmental impact.

Data Source 3: Climate-related Expenditure

	•	Source: Eurostat API
	•	Metadata URL: Eurostat Metadata
	•	Data URL: Eurostat Data
	•	Data Type: TSV
	•	Description: Provides data on climate-related expenditure, enabling assessment of financial contributions to environmental initiatives.

## Work Packages

WP1: Project Setup

	•	Task 1.1: Create a GitHub repository for the project.
	•	Task 1.2: Set up the project structure with necessary directories (/project, /data, /output).
	•	Task 1.3: Add a .gitignore file to exclude data files and other non-essential items.

WP2: Data Extraction

	•	Task 2.1: Write a Python script (pipeline.py) to download datasets from the specified URLs.
	•	Task 2.2: Verify the integrity of the downloaded data.

WP3: Data Transformation

	•	Task 3.1: Clean and preprocess the data (e.g., handle missing values, clean column names).
	•	Task 3.2: Implement transformations to standardize the datasets.
	•	Task 3.3: Transpose datasets as needed and filter data for the years 2014-2022.

WP4: Data Loading

	•	Task 4.1: Save the cleaned data to Excel files in the /output directory.
	•	Task 4.2: Save the cleaned data to SQLite databases in the /output directory.

WP5: Automation and Testing

	•	Task 5.1: Create a shell script (pipeline.sh) to automate the execution of the data pipeline.
	•	Task 5.2: Write unit tests (test_pipeline.py) to validate the data pipeline.
	•	Task 5.3: Create a test execution script (tests.sh) to automate running the unit tests.

WP6: Documentation and Reporting

	•	Task 6.1: Document the data pipeline process in project-plan.md.
	•	Task 6.2: Prepare initial data analysis and visualizations.
	•	Task 6.3: Write a summary report of findings based on the analysis.



