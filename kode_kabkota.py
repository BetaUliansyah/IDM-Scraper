import requests
requests.packages.urllib3.disable_warnings() 
from bs4 import BeautifulSoup
import json
import pandas as pd
from time import perf_counter
start_time = perf_counter()
s = requests.Session()
provinsi_df = pd.read_csv('kode_provinsi.csv')
kode_provinsi = provinsi_df['kode_provinsi']
kabkota_df = pd.DataFrame(columns= ['kode_kabkota', 'nama_kabkota'])
for i in kode_provinsi:
    url = 'https://idm.kemendesa.go.id/open/api/lookup/kabupaten/'+str(i)+'?tahun=2024'
    r = s.get(url, verify=False)
    response_json = json.loads(r.text)
    for j in response_json['mapData']:
        row_df = pd.DataFrame({'kode_kabkota': [j['id']], 'nama_kabkota': [j['text']]})
        kabkota_df = pd.concat([kabkota_df, row_df], ignore_index=True)
kabkota_df.to_csv('kode_kabkota.csv')
end_time = perf_counter()
total_time = end_time - start_time
rows = len(kabkota_df)
print(f"\n---Finished scraping {rows} rows in: {total_time:.2f} seconds---")
