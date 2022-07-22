import time
import requests
from proj_requests import *
from download_file_google import export
import xml.etree.ElementTree as ET
import hashlib
import os
import psycopg2
from datetime import datetime
from psycopg2 import Error


def get_md5(filename):
    md5_hash = hashlib.md5()
    with open(filename, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
        return md5_hash.hexdigest()


def get_usd_price():
    res = requests.get('https://www.cbr.ru/scripts/XML_daily.asp?date_req=' + datetime.now().strftime(("%d/%m/%Y")))
    res = ET.fromstring(res.text)
    for child in res.iter('Valute'):
        if child.attrib['ID'] == 'R01235':
            return float(child[4].text.replace(',', '.'))


def check_file(filename, refresh=False):
    if os.path.exists(filename):
        with open(filename + '.temp', 'wb') as f:
            f.write(export(google_file_ID))
        if get_md5(filename) != get_md5(filename + '.temp'):
            os.remove(filename)
            os.rename(filename + '.temp', filename)
            insert_DB_from_sheet_file(truncate=True)
        else:
            os.remove(filename + '.temp')
        if refresh:
            insert_DB_from_sheet_file()
    else:
        with open(filename, 'wb') as f:
            f.write(export(google_file_ID))
            insert_DB_from_sheet_file()


def query_db(q):
    try:
        connection = psycopg2.connect(user=user_db,
                                      password=passwd_db,
                                      host=host_db,
                                      port=port_db,
                                      database=database_db)
        cursor = connection.cursor()
        create_table_query = q
        cursor.execute(create_table_query)
        connection.commit()

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def insert_DB_from_sheet_file(truncate=False):
    if truncate:
        query_db('TRUNCATE main;')
    with open(sheet_file) as f:
        f = [[j.replace('\n', '') for j in i.split('\t')] for i in f.readlines()]
    for i in f[1:]:
        try:
            for j in range(len(i)):
                if '.' not in i[j]:
                    i[j] = int(i[j])
            i.append(i[2] * get_usd_price())
            if len(i) == 5:
                query_db(f'INSERT INTO main values {tuple(i)} ON CONFLICT (id) DO UPDATE SET'
                         f' (id,order_id,price_usd,date_delivery,price_rub) = {tuple(i)};')
        except Exception as e:
            print(e)


if __name__ == '__main__':
    query_db('CREATE TABLE IF NOT EXISTS public.main (ID INTEGER PRIMARY KEY,ORDER_ID TEXT NOT NULL UNIQUE,'
             'PRICE_USD INTEGER NOT NULL,DATE_delivery DATE NOT NULL,PRICE_RUB REAL NOT NULL);')
    while True:
        check_file(sheet_file, refresh=True)

        time.sleep(5)
