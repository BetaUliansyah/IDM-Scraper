import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import pandas as pd
from time import perf_counter

requests.packages.urllib3.disable_warnings()

start_time = perf_counter()

# Initialize requests session
s = requests.Session()

# Read kabkota codes from CSV
kabkota_df = pd.read_csv('kode_kabkota.csv')
kode_kabkota = kabkota_df['kode_kabkota']

# Prepare DataFrame to store results
kecamatan_df = pd.DataFrame(columns=['kode_kecamatan', 'nama_kecamatan'])

# Define a function to fetch kecamatan data for a given kode_kabkota
def fetch_kecamatan_data(kabkota_code):
    url = f'https://idm.kemendesa.go.id/open/api/lookup/kecamatan/{kabkota_code}?tahun=2024'
    try:
        r = s.get(url, verify=False)
        response_json = r.json()
        # Parse the response to get kecamatan data
        kecamatan_data = [{'kode_kecamatan': kec['id'], 'nama_kecamatan': kec['text']} for kec in response_json.get('mapData', [])]
        return pd.DataFrame(kecamatan_data)
    except Exception as e:
        print(f"Failed to fetch data for kabkota_code {kabkota_code}: {e}")
        return pd.DataFrame(columns=['kode_kecamatan', 'nama_kecamatan'])

# Execute the requests concurrently with a max of 10 connections at a time
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_kecamatan_data, code): code for code in kode_kabkota}
    for future in as_completed(futures):
        result_df = future.result()
        kecamatan_df = pd.concat([kecamatan_df, result_df], ignore_index=True)

# Save the results to CSV
kecamatan_df.to_csv('kode_kecamatan.csv', index=False)

# Calculate and print the elapsed time
end_time = perf_counter()
total_time = end_time - start_time
rows = len(kecamatan_df)
print(f"\n---Finished scraping {rows} rows in: {total_time:.2f} seconds---")
