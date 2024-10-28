import requests
requests.packages.urllib3.disable_warnings() 
from bs4 import BeautifulSoup
import pandas as pd
from time import perf_counter
start_time = perf_counter()
s = requests.Session()
r = s.get('https://idm.kemendesa.go.id/rekomendasi', verify=False)
bsoup = BeautifulSoup(r.text, 'html.parser')
provinsi_select = bsoup.find("select", {"id":"kt_select_prov"})
provinsi_options = provinsi_select.find_all("option")
provinsi_df = pd.DataFrame(columns= ['kode_provinsi', 'nama_provinsi'])
for i in provinsi_options:
    row_df = pd.DataFrame({'kode_provinsi': [i['value']], 'nama_provinsi': [i['label']]})
    provinsi_df = pd.concat([provinsi_df, row_df], ignore_index=True)
provinsi_df.to_csv('provinsi.csv')
end_time = perf_counter()
total_time = end_time - start_time
rows = len(provinsi_df)
print(f"\n---Finished scraping {rows} rows in: {total_time:.2f} seconds---")
