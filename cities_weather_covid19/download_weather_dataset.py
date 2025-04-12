import os
import json
import time
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Load API key from .env
load_dotenv()
API_KEY = os.getenv("WEATHER_KEY")
if not API_KEY:
    raise ValueError("API key not found in .env file")

# Load city names from countries_cities.json
with open("countries_cities.json", "r", encoding="utf-8") as file:
    countries_data = json.load(file)

# Extract all city names
city_names = []
for country in countries_data:
    city_names.extend(country.get("cities", []))  # Add all cities from each country

# Remove duplicates and limit requests (API rate limit consideration)
city_names = list(set(city_names))  # Remove duplicates
city_names = city_names[:100]  # Limit to first 100 cities (adjust as needed)

# Function to fetch weather data
def get_weather(city):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        weather_json = response.json()

        if response.status_code == 200:
            # return {
            #     'city': city,
            #     'weather_condition': weather_json['weather'][0].get('description', "Unknown condition"),
            #     'temperature_min': weather_json['main'].get('temp_min', np.nan),
            #     'temperature_max': weather_json['main'].get('temp_max', np.nan)
            # }

            weather_json["city"] = city  # Add city name
            return weather_json  # Return the entire response
        else:
            print(f"Error fetching {city}: {weather_json}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {city}: {e}")
        return None

# Fetch weather for each city
weather_data = []
for idx, city in enumerate(city_names):
    print(f"Fetching weather for {city} ({idx+1}/{len(city_names)})")
    weather = get_weather(city)
    if weather:
        weather_data.append(weather)
    time.sleep(1)  # Avoid API rate limits

# Save structured weather data to JSON
with open("weather_data.json", "w", encoding="utf-8") as outfile:
    json.dump(weather_data, outfile, indent=4)

# Save to CSV
# df = pd.DataFrame(weather_data)
# df.to_csv("weather_data.csv", index=False)

print("Weather data saved as 'weather_data.json' and 'weather_data.csv'")
