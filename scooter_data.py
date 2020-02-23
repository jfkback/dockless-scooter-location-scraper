import schedule
import time
import requests
import json
import pandas as pd

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Gets API URLs and City names from csv
df = pd.read_csv('scooter_api.csv')
df = df[(df['city'] == 'la_region') | (df['city'] == 'washington_dc')]
urls = df.to_dict(orient='records')

# Sets up google auth and pydrive
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

fid_lookup = {
    'la_region': '1SaX_dgSllD9EKmNeoDKRGiRDwoFB7zu4',
    'washington_dc': '1Xz7BKzOe92-33s41d7dfc-NUGCfb4n4g'
}

def get_scooter_data():
    for url in urls:
        r = requests.get(url['gbfs_freebike_url'])
        if r.status_code == 200:
            print(f"{url['city']}-{url['provider']}-{int(time.time())} SUCCESS")
            json_data = r.json()
            file_name = f"data/{url['city']}-{url['provider']}-{int(time.time())}.json"
            with open(file_name, 'w') as outfile:
                json.dump(json_data, outfile)
            fid = fid_lookup[url['city']]
            f = drive.CreateFile({'parents': [{'kind': 'drive#fileLink', 'id': fid}]})
            f.SetContentFile(file_name)
            f.Upload()
        else:
            print(f"{url['city']}-{url['provider']}-{int(time.time())} FAILED")
            print(f"Status code: {r.status_code}")


def print_gdrive_folders(fid):
    file_list = drive.ListFile({'q': f"'{fid}' in parents and trashed=false"}).GetList()
    for file1 in file_list:
        print ('title: %s, id: %s' % (file1['title'], file1['id']))


schedule.every(10).minutes.do(get_scooter_data)

while True:
    schedule.run_pending()
    time.sleep(1)
