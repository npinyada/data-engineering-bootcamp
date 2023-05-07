import requests
import pandas as pd

API_URL = "http://34.87.139.82:8000/"

DATAS = ['orders', 'products', 'promos', 'users' ,'addresses', 'events', 'order-items']
DATE = "2021-02-10"

for each in DATAS:
    print(f'Start Call API {each}....')
    response = requests.get(f"{API_URL}/{each}/?created_at={DATE}")
    df = pd.DataFrame(response.json())
    df.to_csv(f'data/{each}-api.csv', index=False)
print('Run Completed')