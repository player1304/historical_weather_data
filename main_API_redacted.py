import requests
import pandas as pd
import datetime
import time
import urllib.parse
import json
import os
import csv

API_KEY = "[REDACTED]"
CITIES = ["Shenzhen", "Shanghai", "Guangzhou", "Beijing"]
DATE_RANGE = "20230101-20230331"
OUTPUT_FILE = "weather_data.csv"

def get_unix_timestamp(date_str):
    """Convert YYYYMMDD to Unix timestamp at 06:00 UTC"""
    date = datetime.datetime.strptime(date_str, "%Y%m%d")
    date = date.replace(hour=6, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)
    return int(date.timestamp())

def flatten_json(json_obj, prefix=''):
    """Flatten a nested JSON object"""
    result = {}
    for key, value in json_obj.items():
        if isinstance(value, dict):
            result.update(flatten_json(value, prefix=f"{prefix}{key}_"))
        elif isinstance(value, list):
            if key == "weather" and all(isinstance(i, dict) for i in value):
                # Special handling for the "weather" list of dictionaries
                for i, weather_item in enumerate(value):
                    result.update({f"{prefix}{key}_{k}": v for k, v in weather_item.items()})
            elif all(isinstance(i, (int, float, str)) for i in value):
                result[f"{prefix}{key}"] = ', '.join(map(str, value))
            else:
                # For other types of lists, convert to JSON string
                result[f"{prefix}{key}"] = json.dumps(value)
        else:
            result[f"{prefix}{key}"] = value
    return result

def get_city_coordinates(city):
    """Get latitude and longitude of a city"""
    encoded_city = urllib.parse.quote(city, safe='')
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={encoded_city}&limit=1&appid={API_KEY}"
    response = requests.get(url)
    data = response.json()
    if data:
        return data[0]["lat"], data[0]["lon"]
    else:
        print(f"No data found for {city}")
        return None, None

def get_weather_data(city, lat, lon, date_str):
    """Get weather data for a city on a specific date"""
    timestamp = get_unix_timestamp(date_str)
    url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={timestamp}&units=metric&appid={API_KEY}"
    response = requests.get(url)
    data = response.json()
    if "data" in data:
        flattened_data = flatten_json(data["data"][0])

        # add two columns for easier processing in Excel
        flattened_data["city_name"] = city
        flattened_data["date"] = date_str

        return flattened_data
    else:
        print(f"No data found for {city} on {date_str}")
        return None

def confirm_overwrite(file_path):
    """Prompt user to confirm overwriting an existing file"""
    if os.path.exists(file_path):
        response = input(f"The file '{file_path}' already exists. Do you want to overwrite it? Type 'Y' to confirm: ")
        return response.lower() == 'y'
    return True

def write_csv_header(file_path, fieldnames):
    """Write the CSV header"""
    with open(file_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

def append_to_csv(file_path, data, fieldnames):
    """Append a row to the CSV file"""
    with open(file_path, 'a', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow(data)

def update_csv_with_new_column(file_path, new_fieldnames):
    """Update the CSV file with a new column"""
    temp_file = file_path + '.temp'
    with open(file_path, 'r', newline='', encoding='utf-8-sig') as csvfile, \
         open(temp_file, 'w', newline='', encoding='utf-8-sig') as tempfile:
        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(tempfile, fieldnames=new_fieldnames)
        writer.writeheader()
        for row in reader:
            for field in new_fieldnames:
                if field not in row:
                    row[field] = "NA"
            writer.writerow(row)
    os.replace(temp_file, file_path)

def main():
    if not confirm_overwrite(OUTPUT_FILE):
        print("Operation cancelled.")
        return

    city_coords = {city: get_city_coordinates(city) for city in CITIES}

    start_date, end_date = DATE_RANGE.split("-")
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    fieldnames = None

    for date in date_range:
        for city, (lat, lon) in city_coords.items():
            if lat is not None and lon is not None:
                date_str = date.strftime("%Y%m%d")
                data = get_weather_data(city, lat, lon, date_str)
                if data:
                    if fieldnames is None:
                        fieldnames = list(data.keys())
                        write_csv_header(OUTPUT_FILE, fieldnames)
                    elif set(data.keys()) != set(fieldnames):
                        new_fields = set(data.keys()) - set(fieldnames)
                        missing_fields = set(fieldnames) - set(data.keys())
                        
                        if new_fields:
                            fieldnames.extend(new_fields)
                            update_csv_with_new_column(OUTPUT_FILE, fieldnames)
                            print(f"New column(s) added: {', '.join(new_fields)}")
                        
                        if missing_fields:
                            print(f"For debug: some columns are missing: {', '.join(missing_fields)}. No need to worry.")
                    
                    # Ensure all fieldnames are present in data
                    for field in fieldnames:
                        if field not in data:
                            data[field] = "NA"
                    
                    append_to_csv(OUTPUT_FILE, data, fieldnames)
                    print(f"Data for {city} on {date_str} appended to {OUTPUT_FILE}")
                time.sleep(1)  # To avoid hitting API rate limits

    print(f"Data collection complete. All data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()