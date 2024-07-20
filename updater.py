import os
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Function to download the URL, called asynchronously by several child processes
def download_url(url, download_path):
    target_file_path = os.path.join(download_path, os.path.basename(url))
    if os.path.exists(target_file_path):
        print(f"File already exists: {url}")
        return

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return

    if response.status_code == 404:
        print(f"File not found: {url}")
    else:
        # Create the entire path if it doesn't exist
        os.makedirs(os.path.dirname(target_file_path), exist_ok=True)

        with open(target_file_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {url} to {target_file_path}")

def download_binance_minute_data(cm_or_um, symbols, interval, year, month, download_path):
    if cm_or_um not in ["cm", "um"]:
        print("CM_OR_UM can be only 'cm' or 'um'")
        return

    base_url = f"https://data.binance.vision/data/futures/{cm_or_um}/daily/klines"
    
    # Calculate the number of days in the given month and year
    try:
        last_day = (datetime(year, month % 12 + 1, 1) - timedelta(days=1)).day
    except ValueError:
        print(f"Invalid date for year {year} and month {month}")
        return

    with ThreadPoolExecutor() as executor:
        for symbol in symbols:
            for day in range(1, last_day + 1):  # Adjust days according to the last day of the month
                url = f"{base_url}/{symbol}/{interval}/{symbol}-{interval}-{year}-{month:02d}-{day:02d}.zip"
                executor.submit(download_url, url, download_path)
