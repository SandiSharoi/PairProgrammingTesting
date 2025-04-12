import requests
import pandas as pd
import numpy as np
import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Function to fetch JSON from a given URL
def extract_json_from_url(url: str):
    try:
        print(f"Extracting data from {url} ...")
        return requests.get(url).json()
    except Exception as e:
        raise ConnectionError(f"Can't extract data: {e}")

# Fetch COVID-19 dataset
def get_covid_data() -> pd.DataFrame:
    covid_url = "https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/latest/owid-covid-latest.json"
    print("Fetching COVID-19 dataset ...")
    covid_json = extract_json_from_url(covid_url)
    df_lst = []

    for country_code in covid_json.keys():
        single_country_df = pd.json_normalize(covid_json[country_code])
        single_country_df['iso_country_code'] = country_code
        df_lst.append(single_country_df)
    
    all_countries_df = pd.concat(df_lst, ignore_index=True)
    all_countries_df.replace(r"^\s*$", np.nan, regex=True, inplace=True)
    all_countries_df.dropna(how='all', axis=1, inplace=True)

    selected_cols = ["iso_country_code", "continent", "location", "last_updated_date", 
                     "total_cases", "new_cases", "total_deaths", "new_deaths", 
                     "total_cases_per_million", "total_deaths_per_million", "hosp_patients"]
    
    selected_df = all_countries_df[selected_cols]
    print(f"COVID-19 dataset extracted. Shape: {selected_df.shape}")
    
    return selected_df

# Fetch cities dataset
def get_cities_data() -> pd.DataFrame:
    cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/countries%2Bcities.json"
    print("Fetching cities dataset ...")
    cities_json = extract_json_from_url(cities_url)

    required_data = []
    for country in cities_json:
        data_dict = {
            'country_id': country.get('id', np.nan),
            'country_name': country.get('name', np.nan),
            'country_iso3': country.get('iso3', np.nan),
            'country_capital': country.get('capital', np.nan),
            'country_subregion': country.get('subregion', np.nan),
            'country_region': country.get('region', np.nan),
        }
        required_data.append(data_dict)

    required_df = pd.DataFrame(required_data)
    print(f"Cities dataset extracted. Shape: {required_df.shape}")
    
    return required_df

# Fetch weather data for a city
def get_city_weather(city_name: str) -> dict:
    api_key = os.getenv("WEATHER_KEY")
    weather_api = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric"
    
    try:
        weather_json = extract_json_from_url(weather_api)
        required_dict = {
            "condition": weather_json['weather'][0].get('description', "Unknown weather"),
            "temperature_min": weather_json['main'].get('temp_min', np.nan),
            "temperature_max": weather_json['main'].get('temp_max', np.nan),
            "city": city_name
        }
        return required_dict
    except ConnectionError:
        logging.warning(f"Failed to fetch weather data for {city_name}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error for {city_name}: {e}")
        return None

# Fetch weather data for all capital cities
def get_all_cities_weather(city_names_lst: list) -> pd.DataFrame:
    print("Fetching weather data for all capital cities ...")
    all_data = []

    for idx, city_name in enumerate(city_names_lst):
        print(f"Fetching weather for {city_name} ({idx+1}/{len(city_names_lst)}) ...")
        city_weather = get_city_weather(city_name)
        if city_weather:
            all_data.append(city_weather)

    all_city_df = pd.DataFrame(all_data)
    print(f"Weather dataset extracted. Shape: {all_city_df.shape}")
    
    return all_city_df

# Merge datasets (Cities + Weather + COVID-19)
def transform_data():
    print("\nStarting data transformation ...")
    
    city_df = get_cities_data()
    capital_names = city_df['country_capital'].to_list()
    
    weather_df = get_all_cities_weather(capital_names)
    city_weather_df = city_df.merge(weather_df, how="inner", left_on='country_capital', right_on='city')

    covid_df = get_covid_data()
    covid_city_weather_df = covid_df.merge(city_weather_df, how="left", left_on="iso_country_code", right_on="country_iso3")
    
    print(f"Data transformation completed. Final shape: {covid_city_weather_df.shape}")
    
    return covid_city_weather_df

# Load data into SQLite
def load_data(df: pd.DataFrame):
    print("Loading data into SQLite database ...")
    engine = create_engine('sqlite:///w9_demo.db')
    df.to_sql("covid_city_demo", engine, if_exists="replace")
    print("Successfully loaded into SQLite database!")

# Run ETL process
if __name__ == "__main__":
    print("\n=== Starting ETL Process ===")
    df = transform_data()
    print("\nFinal DataFrame Shape:", df.shape)
    load_data(df)
    print("=== ETL Process Completed Successfully ===")
