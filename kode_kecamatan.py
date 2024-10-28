import requests
requests.packages.urllib3.disable_warnings() 
from bs4 import BeautifulSoup
import json
import pandas as pd
from time import perf_counter
start_time = perf_counter()
s = requests.Session()
kabkota_df = pd.read_csv('kabkota.csv')
kode_kabkota = kabkota_df['kode_kabkota']
kecamatan_df = pd.DataFrame(columns= ['kode_kecamatan', 'nama_kecamatan'])
for i in kode_kabkota:
    url = 'https://idm.kemendesa.go.id/open/api/lookup/kecamatan/'+str(i)+'?tahun=2024'
    r = s.get(url, verify=False)
    response_json = json.loads(r.text)
    for j in response_json['mapData']:
        row_df = pd.DataFrame({'kode_kecamatan': [j['id']], 'nama_kecamatan': [j['text']]})
        kecamatan_df = pd.concat([kecamatan_df, row_df], ignore_index=True)
kecamatan_df.to_csv('kecamatan.csv')
end_time = perf_counter()
total_time = end_time - start_time
rows = len(kecamatan_df)
print(f"\n---Finished scraping {rows} rows in: {total_time:.2f} seconds---")
