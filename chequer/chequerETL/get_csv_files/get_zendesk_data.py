# -*- coding:utf-8 -*-

import pandas as pd
import requests


def get_zendesk_data(user, pwd, tables):
    for tablename in tables:
        url = '' + tablename + '.json?limit=10000000'
        response = requests.get(url, auth=(user, pwd))
        if response.status_code != 200:
            print('Status:', response.status_code, 'Problem with the request. Exiting.')
            exit()
        data = response.json()

        if tablename == 'ticket_audits':
            data_frame = data['audits']
        else:
            data_frame = data[tablename]
        df = pd.DataFrame(data=data_frame)
        df.columns = [col.split(':')[-1] for col in df.columns]
        df.to_csv('/home/ec2-user/airflow/csv_files/zendesk/' + tablename + '.tsv', sep=',', quotechar="'")