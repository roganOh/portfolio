# -*- coding:utf-8 -*-

import pandas as pd
import requests


def get_data(access_token, tables):
    for tablename in tables:
        headers = {
            'Authorization': 'Bearer ' + access_token,
            'Accept': 'application/json',
        }
        params = (
            ('order', 'desc'),
            ('sort', 'updated_at'),
        )
        if tablename == 'contacts':
            response = requests.get('https://api.intercom.io/' + tablename, headers=headers, params=params)
            data = response.json()
            data_frame = data[tablename]
            df = pd.DataFrame(data=data_frame)
            df.columns = [col.split(':')[-1] for col in df.columns]
            df.to_csv('/home/ec2-user/airflow/csv_files/intercom/' + tablename + '.tsv', sep=',', quotechar="'",
                      index=False)

            for i in range(2, data['pages']['total_pages'] + 1):
                print(i)
                response = requests.get('https://api.intercom.io/' + tablename + '?page=' + str(i), headers=headers,
                                        params=params)
                data = response.json()
                data_frame = data[tablename]
                df = pd.DataFrame(data=data_frame)
                df.columns = [col.split(':')[-1] for col in df.columns]
                with open('/home/ec2-user/airflow/csv_files/intercom/' + tablename + '.tsv', 'a') as f:
                    df.to_csv(f, mode='a', sep=',', quotechar="'", header=False, index=False)
        else:
            response = requests.get('https://api.intercom.io/' + tablename, headers=headers, params=params)
            data = response.json()
            data_frame = data[tablename]
            df = pd.DataFrame(data=data_frame)
            df.columns = [col.split(':')[-1] for col in df.columns]
            df.to_csv('/home/ec2-user/airflow/csv_files/intercom/' + tablename + '.tsv', sep=',', quotechar="'",
                      index=False)

            for i in range(2, data['pages']['total_pages'] + 1):
                print(i)
                response = requests.get('https://api.intercom.io/' + tablename + '?page=' + str(i), headers=headers,
                                        params=params)
                data = response.json()
                data_frame = data[tablename]
                df = pd.DataFrame(data=data_frame)
                df.columns = [col.split(':')[-1] for col in df.columns]
                with open('/home/ec2-user/airflow/csv_files/intercom/' + tablename + '.tsv', 'a') as f:
                    df.to_csv(f, mode='a', sep=',', quotechar="'", header=False, index=False)
