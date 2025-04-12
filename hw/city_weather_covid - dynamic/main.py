import os
import json
import pandas as pd
from utils.etl_utils import extract_covid, extract_cities, extract_weather_data, transform_final_df, load_to_files

# Paths to local JSON files
covid_json_path = "covid19.json"
cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"

def get_top_3_death_countries_from_local(file_path: str):
    with open(file_path, "r") as f:
        data = json.load(f)
    
    countries = [
        {"country_code": code, "country_name": value["location"], "total_deaths": value.get("total_deaths") or 0}
        for code, value in data.items()
        if value.get("continent")  # Filter out continents like "OWID_AFR"
    ]
    
    sorted_countries = sorted(countries, key=lambda x: x["total_deaths"], reverse=True)
    top_3 = sorted_countries[:3]
    return [country["country_name"] for country in top_3]

# Extracting data
def main():
    print("🟡 Starting ETL pipeline...")

    print("\n📥 Step 1: Extracting COVID data from local JSON...")
    covid_df = extract_covid(covid_json_path)
    print(f"✅ COVID data extracted: {covid_df.shape[0]} rows")

    top_3_country_names = get_top_3_death_countries_from_local(covid_json_path)
    print(f"\n🌍 Top 3 countries with highest COVID deaths: {top_3_country_names}")

    print("\n📥 Step 2: Extracting cities data...")
    cities_df = extract_cities(cities_url, top_3_country_names)
    print(f"✅ Cities data extracted: {cities_df.shape[0]} rows")

    print("\n🌦️ Step 3: Extracting weather data for each city (this may take some time)...")
    weather_df = extract_weather_data(cities_df)
    print(f"✅ Weather data extracted: {weather_df.shape[0]} rows")

    print("\n🛠️ Step 4: Transforming and merging final dataset...")
    final_df = transform_final_df(covid_df, weather_df)
    print(f"✅ Final data prepared: {final_df.shape[0]} rows, {final_df.shape[1]} columns")

    print("\n💾 Step 5: Saving data to CSV and Excel files...")
    load_to_files(final_df, 'City_Weather_Covid_Data')

    print("\n🎉 ETL process completed successfully!")

if __name__ == '__main__':
    main()
