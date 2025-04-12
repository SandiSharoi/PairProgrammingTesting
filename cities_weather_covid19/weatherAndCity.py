import requests
import pandas as pd

API_key = "ad981a730ced31b2b48774e0b12e6aa8"

# If you want to search for Yangon
city_name = "Yangon"

weather_api = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={API_key}"

# Fetch data
weather_raw_data = requests.get(weather_api).json()

# Convert the JSON response to a DataFrame
weather_df = pd.json_normalize(weather_raw_data)  # Normalizing nested JSON

print(weather_df.head(2))