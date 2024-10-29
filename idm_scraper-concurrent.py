import requests
requests.packages.urllib3.disable_warnings() 
from bs4 import BeautifulSoup
import pandas as pd
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pytz import timezone
start_time = perf_counter()

tahun = 2024
filename = f"data-idm-{tahun}-" + datetime.now(timezone('Asia/Jakarta')).strftime("%Y-%m-%d--%H-%M") + ".csv"

# Initialize requests session
s = requests.Session()

# Read kode desa from CSV
desa_df = pd.read_csv('kode_desa.csv')
kode_desa = desa_df['kode_desa'].iloc[200:205]

# Prepare the DataFrame to store results
idm_df = pd.DataFrame(columns=['tahun', 'kode_desa', 'nama_desa', 'skor_idm', 'status_idm', 'target_status', 'idm_minimal', 'dibutuhkan', 'IKS', 'IKE', 'IKL'])

# Define a function to fetch IDM for a given kode_desa
def fetch_idm(kode_desa, tahun=2024):
    url = url = f'https://idm.kemendesa.go.id/open/api/desa/rumusanpokok/{kode_desa}/{tahun}'
    try:
        r = s.get(url, verify=False)
        bsoup = BeautifulSoup(r.text, 'html.parser')
        # Parse the response to get desa data
        desa = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').text.replace(': ',"")
        skor_idm = bsoup.find("section", {'class': 'content'}).findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
        status_idm = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
        target_status = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
        idm_minimal = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
        dibutuhkan = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
        IKS = bsoup.find('td', string='IKS 2024').findNext('td').text.strip()
        IKE = bsoup.find('td', string='IKE 2024').findNext('td').text.strip()
        IKL = bsoup.find('td', string='IKL 2024').findNext('td').text.strip()
        
        # Append data to DataFrame
        data_desa = pd.DataFrame({
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
        
        return data_desa
    except Exception as e:
        print(f"Failed to fetch data for kode_desa {kode_desa}: {e}")
        return pd.DataFrame(columns=['kode_desa'])

# Execute the requests concurrently with a max of 10 connections at a time
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_idm, code): code for code in kode_desa}
    for future in as_completed(futures):
        result_df = future.result()
        idm_df = pd.concat([idm_df, result_df], ignore_index=True)

# Save the results to CSV
idm_df.to_csv(filename, index=False)

# Calculate and print the elapsed time
end_time = perf_counter()
total_time = end_time - start_time
rows = len(idm_df)
print(f"\n---Finished scraping {rows} rows in: {total_time:.2f} seconds---")
