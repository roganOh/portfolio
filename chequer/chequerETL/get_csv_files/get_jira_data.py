# -*- coding:utf-8 -*-
admin = ''
pwd = ''
jira_server = ""
access_token = ''
import json

import pandas as pd
import requests

url = jira_server + '/rest/api/2/search?startAt=0&maxResults=100'  # for문으로 밖엔.....
response = requests.get(url, auth=(admin, access_token))
data = response.json()
maxrange = data['total']
ranges = int(maxrange / 100) + 1
flag_project_versions = 0
issueid = []
created = []
updated = []
statuscategorychangedate = []


def get_data(admin, pwd, jira_server, access_token):
    flag_project_versions = 0
    tables = ['project', 'project_type', 'resolution', 'role', 'users', 'issues']
    for tablename in tables:
        print(tablename)
        if tablename in ('resolution', 'role'):
            url = jira_server + '/rest/api/2/' + tablename + '?maxResults=100'
            response = requests.get(url, auth=(admin, access_token))
            data = response.json()
            dataframe = data
            df = pd.DataFrame(data=dataframe)
            df.columns = [col.split(':')[-1] for col in df.columns]
            df.to_csv('/home/ec2-user/airflow/csv_files/jira/' + tablename + '.tsv', sep=',', quotechar="'",
                      index=False)  # resolution,role은 startat안먹힘
        elif tablename == 'users':
            j = 0
            for k in range(ranges):
                url = jira_server + '/rest/api/2/' + tablename + '?startAt=' + str(k * 100 + 0) + '&maxResults=100'
                response = requests.get(url, auth=(admin, access_token))
                data = response.json()
                dataframe = data
                df = pd.DataFrame(data=dataframe)
                df.columns = [col.split(':')[-1] for col in df.columns]
                if dataframe == []:
                    break
                if j == 0:
                    df.to_csv('/home/ec2-user/airflow/csv_files/jira/' + tablename + '.tsv', sep=',', quotechar="'",
                              index=False)
                    j = 1
                else:
                    with open('/home/ec2-user/airflow/csv_files/jira/' + tablename + '.tsv', 'a') as f:
                        df.to_csv(f, mode='a', sep=',', quotechar="'", header=False, index=False)

        elif tablename == 'project':
            j = 0
            url = jira_server + '/rest/api/2/' + tablename + '?startAt=' + str(j * 100 + 0) + '&maxResults=100'
            response = requests.get(url, auth=(admin, access_token))
            data = response.json()
            dataframe = data
            df = pd.DataFrame(data=dataframe)
            df.columns = [col.split(':')[-1] for col in df.columns]
            if dataframe == []:
                break
            if j == 0:
                df.to_csv('/home/ec2-user/airflow/csv_files/jira/' + tablename + '.tsv', sep=',', quotechar="'",
                          index=False)
            else:
                with open('/home/ec2-user/airflow/csv_files/jira/' + tablename + '.tsv', 'a') as f:
                    df.to_csv(f, mode='a', sep=',', quotechar="'", header=False, index=False)
            tmp = ['10400']
            ids = df.loc[:, ['id']]
            ids = ids.values.tolist()
            for k in range(len(ids)):
                tmp.append(ids[k][0])
            projectid = []
            for k in tmp:
                if k not in projectid:
                    projectid.append(k)
            for i in range(len(projectid)):
                url = jira_server + '/rest/api/3/project/' + projectid[i] + '/versions'
                response = requests.get(url, auth=(admin, access_token))
                data = response.json()
                dataframe = data
                if dataframe == []:
                    continue
                df = pd.DataFrame(data=dataframe)
                df.columns = [col.split(':')[-1] for col in df.columns]
                if flag_project_versions == 0:
                    df.to_csv('/home/ec2-user/airflow/csv_files/jira/project_versions.tsv', sep=',', header=True,
                              quotechar="'", index=False)
                    if dataframe != []:
                        flag_project_versions = 1
                else:
                    with open('/home/ec2-user/airflow/csv_files/jira/project_versions.tsv', 'a') as f:
                        df.to_csv(f, mode='a', sep=',', quotechar="'", header=False, index=False)  # project_versions
        elif tablename == 'project_type':
            url = jira_server + '/rest/api/2/project/type'
            response = requests.get(url, auth=(admin, access_token))
            data = response.json()
            dataframe = data
            df = pd.DataFrame(data=dataframe)
            df.columns = [col.split(':')[-1] for col in df.columns]
            df.to_csv('/home/ec2-user/airflow/csv_files/jira/' + tablename + '.tsv', sep=',', quotechar="'",
                      index=False)

        elif tablename == 'users':
            for j in range(0, ranges):
                url = jira_server + '/rest/api/2/' + tablename + '?startAt=' + str(j * 100 + 0) + '&maxResults=100'
                response = requests.get(url, auth=(admin, access_token))
                data = response.json()
                dataframe = data
                df = pd.DataFrame(data=dataframe)
                df.columns = [col.split(':')[-1] for col in df.columns]
                if dataframe == []:
                    break
                if j == 0:
                    df.to_csv('/home/ec2-user/airflow/csv_files/jira/' + tablename + '.tsv', sep=',', quotechar="'",
                              index=False)
                else:
                    with open('/home/ec2-user/airflow/csv_files/jira/' + tablename + '.tsv', 'a') as f:
                        df.to_csv(f, mode='a', sep=',', quotechar="'", header=False, index=False)


        elif tablename == 'issues':
            for j in range(0, ranges):
                url = jira_server + '/rest/api/2/search?startAt=' + str(j * 100 + 0) + '&maxResults=100'
                response = requests.get(url, auth=(admin, access_token))
                data = response.json()
                dataframe = data['issues']
                df = pd.DataFrame(data=dataframe)
                df.columns = [col.split(':')[-1] for col in df.columns]
                print(df.columns)
                if dataframe == []:
                    break
                if j == 0:
                    df.to_csv('/home/ec2-user/airflow/csv_files/jira/' + tablename + '.tsv', sep=',', quotechar="'",
                              index=False)
                else:
                    with open('/home/ec2-user/airflow/csv_files/jira/' + tablename + '.tsv', 'a') as f:
                        df.to_csv(f, mode='a', sep=',', quotechar="'", header=False, index=False)
                fields = df.loc[:, ['fields']]
                fields = fields.values.tolist()
                id = df.loc[:, ['id']]
                id = id.values.tolist()
                for i in range(len(id)):
                    issueid.append(id[i][0])
                for i in range(len(fields)):
                    f = json.dumps(fields[i][0])
                    f = json.loads(f)
                    created.append(f["created"])
                    updated.append(f["updated"])
                    statuscategorychangedate.append(f["statuscategorychangedate"])
                df = pd.DataFrame(list(zip(issueid, created, updated, statuscategorychangedate)),
                                  columns=['issueid', 'created', 'updated', 'statuscategorychangedate'])
                df['created'] = df['created'].apply(lambda x: str(x[:19]) + 'Z')
                df['updated'] = df['updated'].apply(lambda x: str(x[:19]) + 'Z')
                df['statuscategorychangedate'] = df['statuscategorychangedate'].apply(lambda x: str(x[:19]) + 'Z')
                df.to_csv('/home/ec2-user/airflow/csv_files/jira/issue_dates.tsv', sep=',', quotechar="'",
                          index=False)  # issue_dates


        else:
            continue
