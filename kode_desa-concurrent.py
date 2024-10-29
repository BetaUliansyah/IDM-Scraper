import requests
requests.packages.urllib3.disable_warnings() 
from bs4 import BeautifulSoup
import json
import pandas as pd
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor, as_completed

start_time = perf_counter()

# Initialize requests session
s = requests.Session()

# Read kecamatan codes from CSV
kecamatan_df = pd.read_csv('kode_kecamatan.csv')
kode_kecamatan = kecamatan_df['kode_kecamatan']

# Prepare the DataFrame to store results
desa_df = pd.DataFrame(columns=['kode_desa', 'nama_desa'])

# Define a function to fetch desa data for a given kode_kecamatan
def fetch_desa_data(kecamatan_code):
    url = f'https://idm.kemendesa.go.id/open/api/lookup/desa/{kecamatan_code}?tahun=2024'
    try:
        r = s.get(url, verify=False)
        response_json = r.json()
        # Parse the response to get desa data
        desa_data = [{'kode_desa': desa['id'], 'nama_desa': desa['text']} for desa in response_json.get('mapData', [])]
        return pd.DataFrame(desa_data)
    except Exception as e:
        print(f"Failed to fetch data for kecamatan_code {kecamatan_code}: {e}")
        return pd.DataFrame(columns=['kode_desa', 'nama_desa'])

# Execute the requests concurrently with a max of 10 connections at a time
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_desa_data, code): code for code in kode_kecamatan}
    for future in as_completed(futures):
        result_df = future.result()
        desa_df = pd.concat([desa_df, result_df], ignore_index=True)

# Save the results to CSV
desa_df.to_csv('kode_desa.csv', index=False)

# Calculate and print the elapsed time
end_time = perf_counter()
total_time = end_time - start_time
rows = len(desa_df)
print(f"\n---Finished scraping {rows} rows in: {total_time:.2f} seconds---") # Approximately take 90 seconds
