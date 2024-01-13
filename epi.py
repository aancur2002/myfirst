import json
import requests
import pyodbc
from datetime import datetime, timedelta
import time
import os

def get_token(client_id, client_secret):
    token = None

    options = {
        'method': 'post',
        'headers': {'Content-Type': 'application/json'},
        'data': json.dumps({
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        })
    }

    try:
        response = requests.post('https://five.epicollect.net/api/oauth/token', **options)
        response.raise_for_status()
        token = response.json()
    except requests.exceptions.RequestException as e:
        pass

    return token

def extract(entries, iteration):
    array = []
    for e in entries:
        temp = []

        if iteration == 1:
            header = [key for key in e.keys()]
            if header and entries[0]:
                array.append(adjust_header_for_gps(header, entries[0]))

        for key in e.keys():
            if e[key].get("latitude") is not None and e[key].get("longitude") is not None and e[key].get("accuracy") is not None:
                if e[key]["longitude"] != "" and e[key]["latitude"] != "" and e[key]["accuracy"] != "":
                    temp.extend([e[key]["accuracy"], e[key]["latitude"], e[key]["longitude"]])
                else:
                    temp.extend(["", "", ""])
            else:
                temp.append(e[key])

        array.append(temp)

    return array

def adjust_header_for_gps(header, random_row):
    counter, so_far = 0, 0
    for key in random_row.keys():
        if random_row[key].get("latitude") is not None and random_row[key].get("longitude") is not None and random_row[key].get("accuracy") is not None:
            index = header.index(key)
            header.insert(1 + counter + so_far * 2, key + "_Longitude")
            header.insert(1 + counter + so_far * 2, key + "_Latitude")
            header.insert(1 + counter + so_far * 2, key + "_Accuracy_m")
            header.pop(index)
            so_far += 1

        counter += 1

    return header

def get_data_and_insert_to_sql():
    # Retrieve API credentials from environment variables
    epicollect_client_id = os.environ.get("2987")
    epicollect_client_secret = os.environ.get("ustmO5mWLbCPdkwVqjAEHWjkbed8u09KBWRZC45G")
    epicollect_survey_name = os.environ.get("daily-sales-tigers-brewery")

    # Retrieve SQL Server credentials from environment variables
    # sql_server_connection_string = os.environ.get("SQL_SERVER_CONNECTION_STRING")

    token = get_token(epicollect_client_id, epicollect_client_secret)

    if token is None:
        return

    options = {
        'muteHttpExceptions': True,
        'method': 'get',
        'headers': {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token['access_token']
        }
    }

    days = 10
    ldate = ''
    if ldate == '':
        l_date = datetime.now()
    else:
        l_date = datetime.strptime(ldate, '%Y-%m-%dT%H:%M:%S.%fZ')

    f_date = l_date - timedelta(days=days)

    url = f'https://five.epicollect.net/api/export/entries/{epicollect_survey_name}' \
          f'?sort_by=created_at&sort_order=DESC&filter_by=created_at&filter_from={f_date.isoformat()}&filter_to={l_date.isoformat()}'
          
    body = json.loads(requests.get(url, **options).text)
    current = body['meta']['current_page']
    last = body['meta']['last_page']
    array = []

    array.extend(extract(body['data']['entries'], 1))

    if current < last:
        for j in range(2, last + 1):
            time.sleep(0.25)
            url = f'https://five.epicollect.net/api/export/entries/{epicollect_survey_name}' \
                  f'?sort_by=created_at&sort_order=DESC&filter_by=created_at&filter_from={f_date.isoformat()}&filter_to={l_date.isoformat()}&page={j}'
            res = json.loads(requests.get(url, **options).text)
            array.extend(extract(res['data']['entries'], j))

    # Connect to SQL Server
    # Assume you have a SQL Server connection
    sql_server_connection_string = "DRIVER={SQL Server};SERVER=your_server;DATABASE=your_database;UID=your_username;PWD=your_password"
    sql_conn = pyodbc.connect(sql_server_connection_string)
    sql_cursor = sql_conn.cursor()

    # Insert data into SQL Server
    for row in array:
        sql_query = f"INSERT INTO YourTable (Column1, Column2, ...) VALUES (?, ?, ...)"
        sql_cursor.execute(sql_query, tuple(row))

    # Commit changes and close connection
    sql_conn.commit()
    sql_conn.close()

# Execute the function to fetch data from Epicollect and insert it into SQL Server
get_data_and_insert_to_sql()
