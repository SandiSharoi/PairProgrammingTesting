import os
import pandas as pd
from utils.etl_utils import extract_covid, extract_cities, extract_weather_data, transform_final_df, load_to_files

covid_url = "https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/latest/owid-covid-latest.json"
cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"

# Extracting data
def main():
    print("🟡 Starting ETL pipeline...")

    print("\n📥 Step 1: Extracting COVID data...")
    covid_df = extract_covid(covid_url)
    print(f"✅ COVID data extracted: {covid_df.shape[0]} rows")

    print("\n🧾 Sample of covid_df:")
    print(covid_df.head())

# Add just below covid_df extraction
    top_3_country_names = ["United States", "Brazil", "India"]

    print("\n📥 Step 2: Extracting cities data...")
    cities_df = extract_cities(cities_url, top_3_country_names)

    print(f"✅ Cities data extracted: {cities_df.shape[0]} rows")

    print("\n🧾 Sample of cities_df:")
    print(cities_df.head())

    print("\n🌦️ Step 3: Extracting weather data for each city (this may take some time)...")
    weather_df = extract_weather_data(cities_df)
    print(f"✅ Weather data extracted: {weather_df.shape[0]} rows")

    print("\n🛠️ Step 4: Transforming and merging final dataset...")
    final_df = transform_final_df(covid_df, weather_df)
    print(f"✅ Final data prepared: {final_df.shape[0]} rows, {final_df.shape[1]} columns")

    print("\n💾 Step 5: Saving data to CSV file...")
    load_to_files(final_df, 'City_Weather_Covid_Data')

    print("\n🎉 ETL process completed successfully!")

if __name__ == '__main__':
    main()
