import subprocess
import sys

# Ensure openpyxl is installed
try:
    import openpyxl
except ImportError:
    print(" 'openpyxl' not found. Installing now...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])
    import openpyxl

import os
import json
from dotenv import load_dotenv

import requests
import pandas as pd
import numpy as np


load_dotenv()


## Extracting data
def extract_covid(file_path: str):
    with open(file_path, "r") as f:
        response = json.load(f)
    covid_country_df = pd.DataFrame(response).T
    covid_country_df.reset_index(inplace=True)
    covid_country_df.rename(columns={'index': 'country_code'}, inplace=True)
    covid_country_df = covid_country_df[["continent", "location", "total_cases", "new_cases", "total_deaths", "new_deaths", "total_cases_per_million", "total_deaths_per_million"]]
    return covid_country_df



def extract_cities(url: str, target_countries: list):
    response = requests.get(url).json()
    raw_cities = []

    for city in response:
        if city.get('country_name') in target_countries:
            raw_cities.append({
                'city': city.get('name', np.nan),
                'country_code': city.get('country_code', np.nan),
                'country_name': city.get('country_name', np.nan),
                'state': city.get('state_name', np.nan),
                'latitude': city.get('latitude', np.nan),
                'longitude': city.get('longitude', np.nan)
            })

    cities_df = pd.DataFrame(raw_cities)
    top_cities = cities_df.groupby('country_name').head(10).reset_index(drop=True)
    return top_cities


def extract_weather_by_city(city: str):
    params = {'q':city, 'appid': os.getenv('WEATHER_KEY'), 'units':'metric'}
    response = requests.get("https://api.openweathermap.org/data/2.5/weather", params=params).json()
    return response

def extract_weather_by_coord(lat: float, lon: float):
    params = {'lat':lat, 'lon':lon, 'appid':os.getenv('WEATHER_KEY'), 'units':'metric'}
    response = requests.get("https://api.openweathermap.org/data/2.5/weather", params=params).json()
    return response

def extract_weather_data(desired_cities_df: pd.DataFrame):
    weather_data = []
    cities_wo_data = []
    desired_cities_df = desired_cities_df.to_dict(orient='records')

    print(f" Total cities to process: {len(desired_cities_df)}\n")
    
    for idx, city in enumerate(desired_cities_df, start=1):
        print(f" [{idx}/{len(desired_cities_df)}] Fetching weather by city: {city['city']}")
        w_data = extract_weather_by_city(city['city'])

        if w_data.get('cod') != 200 or 'weather' not in w_data:
            print(f" Skipping city: {city['city']} — Reason: {w_data.get('message', 'No message')}")
            cities_wo_data.append(city)
            continue

        weather = {
            'City': city['city'],
            'Country': city['country_name'],
            'State': city['state'],
            'Latitude': city['latitude'],
            'Longitude': city['longitude'],
            'Condition': w_data['weather'][0]['description'],
            'Min_Temperature': w_data['main']['temp_min'],
            'Max_Temperature': w_data['main']['temp_max']
        }
        weather_data.append(weather)

    print(f"\n Weather data fetched by city for {len(weather_data)} cities.")
    print(f" Trying coordinates for remaining {len(cities_wo_data)} cities...\n")

    weather_df1 = pd.json_normalize(weather_data, max_level=1)
    weather_data_coord = []

    for idx, city in enumerate(cities_wo_data, start=1):
        lat = city['latitude']
        lon = city['longitude']
        print(f" [{idx}/{len(cities_wo_data)}] Fetching weather by coordinates: {city['city']} ({lat}, {lon})")
        response = extract_weather_by_coord(lat, lon)

        if response.get('cod') != 200 or 'weather' not in response:
            print(f" Failed to get weather by coord for {city['city']} — Reason: {response.get('message', 'No message')}")
            continue

        weather = {
            'City': city['city'],
            'Country': city['country_name'],
            'State': city['state'],
            'Latitude': city['latitude'],
            'Longitude': city['longitude'],
            'Condition': response['weather'][0]['description'],
            'Min_Temperature': response['main']['temp_min'],
            'Max_Temperature': response['main']['temp_max']
        }
        weather_data_coord.append(weather)

    print(f"\n Weather data fetched by coordinates for {len(weather_data_coord)} cities.")

    weather_df2 = pd.json_normalize(weather_data_coord, max_level=1)
    weather_df = pd.concat([weather_df1, weather_df2], axis=0, ignore_index=True)

    print(f"\n Total weather records collected: {len(weather_df)}")
    return weather_df

## Transforming data
def transform_final_df(covid_df: pd.DataFrame, weather_df: pd.DataFrame):
    final_df = covid_df.merge(weather_df, how='inner', left_on='location', right_on='Country')
    final_df.drop(columns=['location'], inplace=True)
    final_df = final_df[['Country','continent', 'City', 'State', 'Latitude', 'Longitude', 'Condition', 'Min_Temperature', 'Max_Temperature', 'total_cases', 'new_cases', 'total_deaths', 'new_deaths', 'total_cases_per_million', 'total_deaths_per_million']]
    return final_df

## Loading data
def load_to_csv(final_df: pd.DataFrame, table_name: str):
    final_df.to_csv(f"{table_name}.csv", index=False, float_format="%.2f", na_rep="N/A")
    print(f" {table_name}.csv saved successfully.")


def load_to_excel(final_df: pd.DataFrame, table_name: str):
    excel_file = f"{table_name}.xlsx"
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        final_df.to_excel(writer, sheet_name="Data", index=False, float_format="%.2f", na_rep="N/A")
    print(f" {excel_file} saved successfully in structured Excel format.")


def load_to_files(final_df: pd.DataFrame, table_name: str):
    print("\n Saving to CSV...")
    load_to_csv(final_df, table_name)

    print("\n Saving to Excel...")
    load_to_excel(final_df, table_name)
