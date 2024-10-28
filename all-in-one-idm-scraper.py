import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from time import perf_counter
from datetime import datetime
from pytz import timezone

requests.packages.urllib3.disable_warnings() 
start_time = perf_counter()

def populate_provinsi():
    s = requests.Session()
    r = s.get('https://idm.kemendesa.go.id/rekomendasi', verify=False)
    bsoup = BeautifulSoup(r.text, 'html.parser')
    #print(bsoup)
    #kode_provinsi = []
    provinsi_select = bsoup.find("select", {"id":"kt_select_prov"})
    provinsi_options = provinsi_select.find_all("option")
    provinsi_df = pd.DataFrame(columns= ['kode_provinsi', 'nama_provinsi'])
    for i in provinsi_options:
        row_df = pd.DataFrame({'kode_provinsi': [i['value']], 'nama_provinsi': [i['label']]})
        provinsi_df = pd.concat([provinsi_df, row_df], ignore_index=True)
    return(provinsi_df)

def populate_kabkota(provinsi_df, tahun=2024):
    kode_provinsi = provinsi_df['kode_provinsi']
    kabkota_df = pd.DataFrame(columns= ['kode_kabkota', 'nama_kabkota'])

    for i in kode_provinsi:
        url = 'https://idm.kemendesa.go.id/open/api/lookup/kabupaten/'+i+'?tahun='+str(tahun)
        r = s.get(url, verify=False)
        response_json = json.loads(r.text)
        for j in response_json['mapData']:
            row_df = pd.DataFrame({'kode_kabkota': [j['id']], 'nama_kabkota': [j['text']]})
            kabkota_df = pd.concat([kabkota_df, row_df], ignore_index=True)
    return(kabkota_df)

def populate_kecamatan(kabkota_df, tahun=2024):
    kode_kabkota = kabkota_df['kode_kabkota']
    kecamatan_df = pd.DataFrame(columns= ['kode_kecamatan', 'nama_kecamatan'])

    for i in kode_kabkota:
        url = 'https://idm.kemendesa.go.id/open/api/lookup/kecamatan/'+i+'?tahun='+str(tahun)
        r = s.get(url, verify=False)
        response_json = json.loads(r.text)
        for j in response_json['mapData']:
            row_df = pd.DataFrame({'kode_kecamatan': [j['id']], 'nama_kecamatan': [j['text']]})
            kecamatan_df = pd.concat([kecamatan_df, row_df], ignore_index=True)
    return(kecamatan_df)

def populate_desa(kecamatandf, tahun=2024):
    kode_kecamatan = kecamatan_df['kode_kecamatan']
    desa_df = pd.DataFrame(columns= ['kode_desa', 'nama_desa'])

    for i in kode_kecamatan:
        url = 'https://idm.kemendesa.go.id/open/api/lookup/desa/'+i+'?tahun='+str(tahun)
        r = s.get(url, verify=False)
        response_json = json.loads(r.text)
        for j in response_json['mapData']:
            row_df = pd.DataFrame({'kode_desa': [j['id']], 'nama_desa': [j['text']]})
            desa_df = pd.concat([desa_df, row_df], ignore_index=True)
    return(desa_df)

def idm_scrape(kode_desa, tahun=2024): # kode desa is a list
    idm_df = pd.DataFrame(columns= ['tahun', 'kode_desa', 'nama_desa', 'skor_idm', 'status_idm', 'target_status', 'idm_minimal', 'dibutuhkan', 'IKS', 'IKE', 'IKL'])
    for i in kode_desa:
        url = 'https://idm.kemendesa.go.id/open/api/desa/rumusanpokok/'+i+'/'+str(tahun)
        r = s.get(url, verify=False)
        bsoup = BeautifulSoup(r.text, 'html.parser')

        kode_desa = i
        desa = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').text.replace(': ',"")
        skor_idm = bsoup.find("section", {'class': 'content'}).findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
        status_idm = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
        target_status = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
        idm_minimal = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
        dibutuhkan = bsoup.find("section", {'class': 'content'}).findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('tr').findNext('td').findNext('td').findNext('td').findNext('td').text.replace(': ',"")
        IKS = bsoup.find('td', string='IKS 2024').findNext('td').text.strip()
        IKE = bsoup.find('td', string='IKE 2024').findNext('td').text.strip()
        IKL = bsoup.find('td', string='IKL 2024').findNext('td').text.strip()
    

    row_df = pd.DataFrame({
        'tahun' : tahun, 
        'kode_desa' : kode_desa, 
        'nama_desa' : desa, 
        'skor_idm' : skor_idm, 
        'status_idm' : status_idm, 
        'target_status' : target_status, 
        'idm_minimal' : idm_minimal, 
        'dibutuhkan': dibutuhkan, 
        'IKS' : IKS, 
        'IKE': IKE, 
        'IKL' : IKL
    }, index=[0])
    
    idm_df = pd.concat([idm_df, row_df], ignore_index=True)
    return(idm_df)

if __name__ == "__main__":
    tahun = 2024
    filename = "data-idm-2024-"+ datetime.now(timezone('Asia/Jakarta')).strftime("%Y-%m-%d--%H-%M") + ".xlsx"
    # populate provinsi
    provinsi_df = populate_provinsi()

    #populate kabkota
    kabkota_df = populate_kabkota(provinsi_df, tahun)

    # populate kecamatan
    kecamatan_df = populate_kecamatan(kabkota_df, tahun)

    # populate desa
    desa_df = populate_desa(kecamatan_df, tahun)
                            
    # IDM Crawler 
    # selected_df = desa_df[desa_df['kode_desa'].str.contains('110101')]
    selected_df = desa_df.iloc[:10]
    print(selected_df)
    idm_df = idm_scrape(selected_df, tahun)

    # end time
    end_time = perf_counter()
    total_time = end_time - start_time
    print(f"\n---Finished in: {total_time:.2f} seconds---")

    # save performance in new sheet
    performance_df = pd.DataFrame({
        'Start time': [start_time],
        'End time': [end_time],
        'Total time': [total_time]
        })

    # save to file
    with pd.ExcelWriter(filename) as writer:
        provinsi_df.to_excel(writer, sheet_name='Provinsi', index=False)
        kabkota_df.to_excel(writer, sheet_name='KabKota', index=False)
        kecamatan_df.to_excel(writer, sheet_name='Kecamatan', index=False)
        desa_df.to_excel(writer, sheet_name='Desa', index=False)
        idm_df.to_excel(writer, sheet_name='IDM', index=False)
        performance_df.to_excel(writer, sheet_name="Info", index=False)
