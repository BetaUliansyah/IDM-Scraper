import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import pandas as pd
from time import perf_counter, sleep
from datetime import datetime
from pytz import timezone

# Disable SSL warnings
requests.packages.urllib3.disable_warnings()

# Set start time for performance measurement
start_time = perf_counter()

# Set parameters
tahun = 2024
filename = f"data-idm-{tahun}-" + datetime.now(timezone('Asia/Jakarta')).strftime("%Y-%m-%d--%H-%M") + ".csv"
max_retries = 3  # Maximum number of retries for failed requests

# Initialize requests session
s = requests.Session()

# Read kode desa from CSV
desa_df = pd.read_csv('kode_desa.csv')
kode_desa = desa_df['kode_desa'].iloc[300:400]

# Prepare the DataFrame to store results
idm_df = pd.DataFrame(columns=['tahun', 'kode_desa', 'nama_desa', 'skor_idm', 'status_idm', 'target_status', 'idm_minimal', 'dibutuhkan', 'IKS', 'IKE', 'IKL'])

# Define a function to fetch IDM data with retry mechanism
def fetch_idm(kode_desa, tahun=2024):
    url = f'https://idm.kemendesa.go.id/open/api/desa/rumusanpokok/{kode_desa}/{tahun}'
    for attempt in range(max_retries):
        try:
            r = s.get(url, verify=False)
            r.raise_for_status()  # Raise an error for HTTP errors

            # Parse the response
            bsoup = BeautifulSoup(r.text, 'html.parser')
            desa = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').text.replace(': ',"")
            skor_idm = bsoup.find("section", {'class': 'content'}).findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
            status_idm = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
            target_status = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
            idm_minimal = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
            dibutuhkan = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
            IKS = bsoup.find('td', string='IKS 2024').findNext('td').text.strip()
            IKE = bsoup.find('td', string='IKE 2024').findNext('td').text.strip()
            IKL = bsoup.find('td', string='IKL 2024').findNext('td').text.strip()
            
            # Return the data as a DataFrame row
            return pd.DataFrame({
                'tahun': [tahun],
                'kode_desa': [kode_desa],
                'nama_desa': [desa],
                'skor_idm': [skor_idm],
                'status_idm': [status_idm],
                'target_status': [target_status],
                'idm_minimal': [idm_minimal],
                'dibutuhkan': [dibutuhkan],
                'IKS': [IKS],
                'IKE': [IKE],
                'IKL': [IKL]
            })
        
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed for kode_desa {kode_desa}: {e}")
            if attempt < max_retries - 1:
                sleep(2*attempt)  # Wait before retrying
            else:
                print(f"Failed to fetch data for kode_desa {kode_desa} after {max_retries} attempts.")
    
    return pd.DataFrame(columns=['kode_desa'])  # Return empty row if all attempts fail

# Execute requests concurrently with a max of 10 connections at a time
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(fetch_idm, code): code for code in kode_desa}
    for future in as_completed(futures):
        result_df = future.result()
        idm_df = pd.concat([idm_df, result_df], ignore_index=True)

# Save results to CSV
idm_df.to_csv(filename, index=False)

# Calculate and print elapsed time
end_time = perf_counter()
total_time = end_time - start_time
rows = len(idm_df)
print(f"\n---Finished scraping {rows} rows in: {total_time:.2f} seconds---")
