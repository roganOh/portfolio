# -*- coding:utf-8 -*-
from datetime import date

import httplib2
import pandas as pd
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

def get_ga_data(viewid, start_date, DIMS, METRICS, locate):
    credentials = ServiceAccountCredentials.from_json_keyfile_name('gs/client_secrets.json',
                                                                   [
                                                                       'https://www.googleapis.com/auth/analytics.readonly'])

    http = credentials.authorize(httplib2.Http())
    service = build('analytics', 'v4', http=http,
                    discoveryServiceUrl=(''))
    today = date.today()

    response = service.reports().batchGet(
        body={

            'reportRequests': [
                {
                    'viewId': viewid,
                    'dateRanges': [{'startDate': start_date, 'endDate': today.strftime('%Y-%m-%d')}],
                    'metrics': [{'expression': exp} for exp in METRICS],
                    'dimensions': [{'name': name} for name in DIMS],
                    'pageSize': 1000000000
                }]
        }
    ).execute()

    data_dic = {f"{i}": [] for i in DIMS + METRICS}
    for report in response.get('reports', []):
        rows = report.get('data', {}).get('rows', [])
        for row in rows:
            for i, key in enumerate(DIMS):
                data_dic[key].append(row.get('dimensions', [])[i])  # Get dimensions
            dateRangeValues = row.get('metrics', [])
            for values in dateRangeValues:
                all_values = values.get('values', [])  # Get metric values
                for i, key in enumerate(METRICS):
                    data_dic[key].append(all_values[i])

    df = pd.DataFrame(data=data_dic)
    df.columns = [col.split(':')[-1] for col in df.columns]
    df['date'] = df['date'].apply(lambda x: datetime.strptime(x, "%Y%m%d").date())
    df.to_csv('/home/ec2-user/airflow/csv_files/' + locate + '.tsv', sep=',', quotechar="'", na_rep='NaN')