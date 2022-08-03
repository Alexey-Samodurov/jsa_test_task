import os
from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from jsa_test_task import get_filenames, months


default_args = {
    'start_date': datetime.now(),
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
    'email': 'damestos988@gmail.com'
}

DATA_ROOT = os.path.join('C:', 'Users', 'dames', 'Downloads', 'тестовое задание')
ETl_MONTHS = "{{ data_interval_end.strftime('%m') }}"

with DAG(
    'by_city_data_extractor',
    description='Собираем список файлов с данными по заданной дате, преобразуем, сохраняем в экселе отчет',
    default_args=default_args,
    schedule_interval='@monthly',
    tags=['by_city_data_extractor']
) as dag:

    def get_csv_data(filenames: list) -> pd.DataFrame:
        df = pd.DataFrame()
        for file in filenames:
            if not pd.read_csv(file).empty:
                df = df.append(pd.read_csv(file))
                warehouse = file.split(os.sep)[len(file.split(os.sep)) - 1].split('_')[0]
                df['Склад'] = warehouse
        print(df.shape)
        if not df.empty:
            return df

    def get_xlsx_data(filenames: list) -> pd.DataFrame:
        df = pd.DataFrame()
        for file in filenames:
            if not pd.read_excel(file).empty:
                df = df.append(pd.read_excel(file))
                warehouse = file.split(os.sep)[len(file.split(os.sep)) - 1].split('_')[0]
                df['Склад'] = warehouse
        print(df.shape)
        if not df.empty:
            return df

    def get_json_data(filenames: list) -> pd.DataFrame:
        df = pd.DataFrame()
        for file in filenames:
            if not pd.read_json(file).empty:
                df = df.append(pd.read_json(file))
                warehouse = file.split(os.sep)[len(file.split(os.sep)) - 1].split('_')[0]
                df['Склад'] = warehouse
        print(df.shape)
        if not df.empty:
            return df

    def preprocess_data(month: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        cols = ['Склад', 'Кол-во, шт', 'Стоимость, руб']
        for num, name in months.items():
            if month == num:
                csv_data = get_csv_data(get_filenames(DATA_ROOT, pattern=f'{name}.csv'))
                csv_data['Стоимость, руб'] = csv_data['Кол-во, шт'] * csv_data['Цена, ед.']
                csv_data = csv_data[cols]
                xlsx_data = get_xlsx_data(get_filenames(DATA_ROOT, pattern=f'{name}.xlsx'))
                xlsx_data['Стоимость, руб'] = xlsx_data['Кол-во, шт'] * xlsx_data['Цена, ед.']
                xlsx_data = xlsx_data[cols]
                json_data = get_json_data(get_filenames(DATA_ROOT, pattern=f'{name}.json'))
                json_data['Стоимость, руб'] = json_data['Кол-во, шт'] * json_data['Цена, ед.']
                json_data = json_data[cols]
                return csv_data, xlsx_data, json_data

    def collect_data(month: int) -> pd.DataFrame:
        tot_df = pd.DataFrame()
        csv_data, xlsx_data, json_data = preprocess_data(month=month)
        for item in [csv_data, xlsx_data, json_data]:
            tot_df = tot_df.append(item)
        return tot_df

    def save_report(month: int):
        df = collect_data(month=int(month))
        df.to_excel(os.path.join(DATA_ROOT, f'result_repot_{month}.xlsx'), index=False)

    save_report_ = PythonOperator(
        task_id='save_report',
        python_callable=save_report,
        op_args=[ETl_MONTHS],
        sla=timedelta(seconds=10)
    )
