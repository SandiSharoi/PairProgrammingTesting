# Final Summary of the Code
# Fetch Data:

# COVID-19 stats from Our World in Data (OWID).

# Country and city data from the Countries+Cities API.

# Weather data from the OpenWeather API (requires an API key stored in environment variables).

# Transform Data:

# Merge city, weather, and COVID data based on country codes (iso_country_code from COVID data and country_iso3 from city data).

# Extracts weather data only for capital cities of each country.

# Converts empty values to NaN for consistency.

# Load Data:

# Store the final merged dataset into an SQLite database (class_demo.db).

# Uses SQLAlchemy to store data in a table.


import requests
import pandas as pd
import numpy as np
import logging
import os

from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables (API keys)
load_dotenv()

# Utility function to print DataFrame details
def print_df(df: pd.DataFrame):
    print(df.shape)  # Print number of rows and columns
    print(df.columns)  # Print column names
    print(df.head())  # Print first 5 rows

# Function to fetch JSON data from a URL
def extract_json_from_url(url: str) -> dict:
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error if request fails
        return response.json()  # Return JSON response
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return {}

# Extract COVID-19 data from OWID
def extract_covid_data():
    covid_url = "https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/latest/owid-covid-latest.json"
    covid_json = extract_json_from_url(covid_url)

# covid_json = {
#     "AFG": {"continent": "Asia", "location": "Afghanistan", "total_cases": 235214},
#     "USA": {"continent": "North America", "location": "United States", "total_cases": 103500000},
#     "INVALID": "This is a string"  # This will be skipped by isinstance() check
# }
    if not covid_json:
        print("Error: Failed to retrieve COVID data.")
        return pd.DataFrame()

    country_dfs_lst = []
    for country_code, country_data in covid_json.items():
        if isinstance(country_data, dict):  # Ensure it's valid JSON(dictionary)
            
            # Converts the nested dictionary into a flat DataFrame.
            single_country_df = pd.json_normalize(country_data)

                # single_country_df = {
                #     "continent": "Asia",
                #     "location": "Afghanistan",
                #     "last_updated_date": "2024-08-04",
                #     "total_cases": 235214,
                #     "new_cases": 0
                # }

            # Adding iso_country_code (e.g., "AFG", "USA").
            single_country_df['iso_country_code'] = country_code

            # Appending to the "country_dfs_lst" List
            # country_dfs_lst is a list of multiple DataFrames
            country_dfs_lst.append(single_country_df)
                # country_dfs_lst = [   {
                #     "continent": "Asia",
                #     "location": "Afghanistan",
                #     "last_updated_date": "2024-08-04",
                #     "total_cases": 235214,
                #     "new_cases": 0
                #   }
                # ]

    if not country_dfs_lst:
        print("Error: No COVID-19 data collected.")
        return pd.DataFrame()

        

# Since country_dfs_lst is a list of multiple DataFrames, calling pd.concat() merges them into a single DataFrame.
# If we don’t use ignore_index=True, the original index from each country’s DataFrame is kept:

# following is if we don't use ignore_index=True
#    continent     location last_updated_date  total_cases  new_cases
# 0      Asia  Afghanistan        2024-08-04      235214          0
# 0      Asia      Myanmar        2024-02-14      500000        100

    all_df = pd.concat(country_dfs_lst, ignore_index=True) 

# following is if we use ignore_index=True
#       continent     location last_updated_date  total_cases  new_cases
# 0      Asia  Afghanistan        2024-08-04      235214          0
# 1      Asia      Myanmar        2024-02-14      500000        100


# Selecting Relevant Columns
    selected_cols = [
        "iso_country_code", "continent", "location", "last_updated_date",
        "total_cases", "new_cases", "total_deaths", "new_deaths",
        "total_cases_per_million", "total_deaths_per_million", "hosp_patients"
    ]
    

    # Filter only selected columns which exist in the DataFrame
    # Prevents errors if OWID changes the dataset structure.
    selected_cols = [col for col in selected_cols if col in all_df.columns]
    select_df = all_df[selected_cols]

    # Handling Missing Data
    # Convert empty strings to NaN safely
    # Replaces empty strings ("") with NaN.
    # r"^\s*$" is a regular expression (regex) that matches empty or whitespace-only strings.

    select_df = select_df.replace(r"^\s*$", np.nan, regex=True)

    # The function returns a Pandas DataFrame containing the cleaned COVID-19 data.
    return select_df
    # iso_country_code	continent	location	last_updated_date	total_cases	new_cases	total_deaths	new_deaths
    # AFG	Asia	Afghanistan	2024-08-04	235214	0	7500	1
    # USA	North America	United States	2024-08-04	103500000	500	1200000	50



# Extract city and country data
def extract_city_data():
    all_data = []
    # city_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/countries%2Bcities.json"
    city_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/json/countries%2Bcities.json" 
    
    city_lst_json = extract_json_from_url(city_url)
    
    # Print the response to debug
    if not city_lst_json:
        print("Error: Empty response from city data API.")
        return pd.DataFrame()
    
    # Ensure it is a list before proceeding
    if not isinstance(city_lst_json, list):
        print(f"Unexpected response format: {type(city_lst_json)}")
        return pd.DataFrame()
    
    for country in city_lst_json:
        selected_data_dict = {
            "country_id": country.get("id", None),
            "country_name": country.get("name", None),
            "country_iso3": country.get("iso3", None),
            "country_capital": country.get("capital", None),
            "country_subregion": country.get("subregion", None),
            "country_region": country.get("region", None),
        }

        

        all_data.append(selected_data_dict)

    all_df = pd.DataFrame(all_data)

    # Remove rows with missing country name
    all_df = all_df.dropna(subset=["country_name"])

    return all_df


# Extract weather data for a single city
def extract_single_city_weather(city_name: str) -> dict:
    api_key = os.getenv("WEATHER_KEY")  # Load API key from .env
    if not api_key:
        print("Missing WEATHER_KEY in environment variables.")
        return None

    weather_api = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric"
    weather_json = extract_json_from_url(weather_api)

    try:
        city_weather_dict = {
            'weather_condition': weather_json['weather'][0].get('description', "Unknown condition"),
            'temperature_min': weather_json['main'].get('temp_min', np.nan),
            'temperature_max': weather_json['main'].get('temp_max', np.nan),
            'city': city_name
        }

        
        return city_weather_dict

    except KeyError as e:
        print(f"Missing key {e} for city {city_name}")
    except Exception as e:
        print(f"Error processing weather data for {city_name}: {e}")

    return None

# Extract weather data for multiple cities
def extract_all_cities_weather(city_names: list):
    all_result_lst = []
    valid_city_names = [city for city in city_names if isinstance(city, str) and city.strip()]

    for i, city_name in enumerate(valid_city_names):
        print(f"Fetching weather for {city_name} ({i+1}/{len(valid_city_names)})")
        if city_dict := extract_single_city_weather(city_name):
            all_result_lst.append(city_dict)
        else:
            print(f"Skipping {city_name} due to missing data.")

    if not all_result_lst:
        print("No valid weather data collected.")
        return pd.DataFrame()

    all_df = pd.DataFrame(all_result_lst)
    return all_df

# Transformation function to merge data
def transform():
    print("Extracting city data...")
    city_df = extract_city_data()
    if city_df.empty:
        print("City data is empty. Exiting transformation.")
        return

    print_df(city_df)

    # Get list of capital cities
    capital_names_lst = city_df['country_capital'].dropna().to_list()

    print("Extracting weather data...")
    weather_df = extract_all_cities_weather(capital_names_lst)
    if weather_df.empty:
        print("Weather data is empty. Skipping weather merge.")
        city_weather_df = city_df
    else:
        print_df(weather_df)
        city_weather_df = city_df.merge(weather_df, how="left", left_on="country_capital", right_on="city")

    print("Extracting COVID data...")
    covid_df = extract_covid_data()
    if covid_df.empty:
        print("COVID data is empty. Exiting transformation.")
        return

    print_df(covid_df)

    print("Ensuring column types match before merging...")
    
    # Convert columns to string before merging
    covid_df["iso_country_code"] = covid_df["iso_country_code"].astype(str)
    city_weather_df["country_iso3"] = city_weather_df["country_iso3"].astype(str)

    print("Merging data...")
    final_df = covid_df.merge(city_weather_df, how="left", left_on="iso_country_code", right_on="country_iso3")

    print_df(final_df)

# Load data into SQLite database
def load_data(df: pd.DataFrame, table_name: str):
    engine = create_engine("sqlite:///class_demo.db")
    df.to_sql(table_name, con=engine, index=False, if_exists="replace")

# Execute the script
if __name__ == "__main__":
    transform()