from typing import Tuple
import os
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import boto3
import redshift_connector


DEFAULT_REDSHIFT_PORT = 5439
ENGINE_PSW = "mysecretpass"


def get_engine() -> Engine:
    engine = create_engine(f"postgresql+psycopg2://postgres:{ENGINE_PSW}@localhost/postgres")
    return engine


def clean_code(text, df_parents: DataFrame) -> Tuple[str, str]:
    text = str(text)
    parent_code = None

    if len(text) == 11:
        code = text[:5]
        parent_code = text[:1]
    else:
        code = text[:6]
        parent_code = text[:2]

    try:
        parent = df_parents[df_parents["Code_comm"] == parent_code]["Description"].values[0]
    except:
        parent = None

    return code, parent


def create_dimension(data, id_name):
    list_keys = []
    value = 1
    for _ in data:
        list_keys.append(value)
        value += 1
    return pd.DataFrame({id_name: list_keys, "values": data})


def get_aws_connection():
    client = boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )
    return client


def get_redshift_connection():
    conn = redshift_connector.connect(
        host=os.environ.get("redshift_host"),
        database=os.environ.get("redshift_database"),
        port=DEFAULT_REDSHIFT_PORT,
        user=os.environ.get("redshift_user"),
        password=os.environ.get("redshift_pass"),
    )

    # cursor = conn.cursor()

    return conn
    # return cursor


def load_file(file_name: str):
    table_name = file_name.split(".")[0]
    client = get_aws_connection()
    client.upload_file(
        Filename="target/{}".format(file_name), Bucket="tv-etl", Key="etl_imp_prod_target/{}".format(file_name)
    )

    sentence = """
        copy etl_test.{} 
        from 's3://tv-etl/etl_imp_prod_target/{}'
        credentials 'aws_access_key_id={}; aws_secret_access_key={}
        csv delimiter '|'
        region 'us-west-2'
        ignoreheader 1
    """.format(
        table_name, file_name, os.environ.get("AWS_ACCESS_KEY_ID"), os.environ.get("AWS_SECRET_ACCESS_KEY")
    )

    conn = get_redshift_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(sentence)
        print("Carga OK en la tabla", table_name)
    except:
        print("Error en la tabla", table_name)
    finally:
        cursor.close()


def load_file_aws_redshift(file_name: str):
    """
    Load file in AWS and copy to Redshift
    """
    load_file(file_name)
