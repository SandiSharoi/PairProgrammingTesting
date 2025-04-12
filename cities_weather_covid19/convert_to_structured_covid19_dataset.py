import json
import pandas as pd

# Load the JSON file
with open("covid-19.json", "r", encoding="utf-8") as file:
    data = json.load(file)

# Convert the nested structure into a dictionary
structured_data = {}
for country_code, details in data.items():
    if isinstance(details, dict):  # Ensure it's a valid country entry
        structured_data[country_code] = details  # Store as key-value pair

# Save as a structured JSON file
with open("structured_covid19.json", "w", encoding="utf-8") as outfile:
    json.dump(structured_data, outfile, indent=4)

# Convert dictionary to DataFrame
df = pd.DataFrame.from_dict(structured_data, orient="index")

# Convert DataFrame to nested JSON and save
df.to_json("structured_covid19_nested.json", orient="index", indent=4)

print("Structured data saved as 'structured_covid19.json' and 'structured_covid19_nested.json'")
