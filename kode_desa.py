import requests
requests.packages.urllib3.disable_warnings() 
from bs4 import BeautifulSoup
import json
import pandas as pd
from time import perf_counter
start_time = perf_counter()
s = requests.Session()
kecamatan_df = pd.read_csv('kode_kecamatan.csv')
kode_kecamatan = kecamatan_df['kode_kecamatan']
desa_df = pd.DataFrame(columns= ['kode_desa', 'nama_desa'])
for i in kode_kecamatan:
    url = 'https://idm.kemendesa.go.id/open/api/lookup/desa/'+str(i)+'?tahun=2024'
    r = s.get(url, verify=False)
    response_json = json.loads(r.text)
    for j in response_json['mapData']:
        row_df = pd.DataFrame({'kode_desa': [j['id']], 'nama_desa': [j['text']]})
        desa_df = pd.concat([desa_df, row_df], ignore_index=True)
desa_df.to_csv('kode_desa.csv')
end_time = perf_counter()
total_time = end_time - start_time
rows = len(desa_df)
print(f"\n---Finished scraping {rows} rows in: {total_time:.2f} seconds---") # Approximately take 340 seconds
