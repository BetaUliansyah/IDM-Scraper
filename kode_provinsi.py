import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
requests.packages.urllib3.disable_warnings() 

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
