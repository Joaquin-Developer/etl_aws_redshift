from typing import Dict, Tuple
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import os
import boto3
import redshift_connector

from src.etl_imp_products import utils


PSW = "mysecretpass"


def get_engine() -> Engine:
    engine = create_engine(f"postgresql+psycopg2://postgres:{PSW}@localhost/postgres")
    return engine


def extract() -> Dict[str, DataFrame]:
    engine = get_engine()
    df_trades = pd.read_sql("SELECT * FROM trades", engine)
    df_countries = pd.read_json("src/country_data.json")
    df_codes = pd.read_csv("src/hs_codes.csv")
    df_parents = df_codes[df_codes["level"] == 2].copy()

    return {"trades": df_trades, "countries": df_countries, "codes": df_codes, "parents": df_parents}


def transform(data: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    df_codes = data["codes"]
    df_parents = data["parents"]
    df_countries = data["countries"]
    df_trades = data["trades"]

    df_codes = df_codes[df_codes["Code_comm"].notnull()]

    df_codes[["clean_code", "parent_description"]] = df_codes.apply(
        lambda x: utils.clean_code(x["Code"], df_parents), axis=1, result_type="expand"
    )

    df_codes = df_codes[df_codes["clean_code"].notnull()]["clean_code", "description", "parent_description"]

    df_codes["id_code"] = df_codes.index + 1
    df_codes["clean_code"] = df_codes["clean_code"].astype("int64")

    df_countries = df_countries[["alpha-3", "country", "region", "sub-region"]]

    df_countries = df_countries[df_countries["alpha-3"].notnull()]
    df_countries["id_country"] = df_countries.index + 1

    df_trades_clean = df_trades.merge(
        df_codes[["clean_code", "id_code"]], how="left", left_on="comm_code", right_on="clean_code"
    ).merge(df_countries[["alpha-3", "id_country"]], how="left", left_on="country_code", right_on="alpha-3")

    df_quantity = utils.create_dimension(df_trades_clean["quantity_name"].unique(), "id_quantity")
    df_flow = utils.create_dimension(df_trades_clean["flow"].unique(), "id_flow")
    df_year = utils.create_dimension(df_trades_clean["year"].unique(), "id_year")

    df_trades_clean = (
        df_trades_clean.merge(df_quantity, how="left", left_on="quantity_name", right_on="values")
        .merge(df_flow, how="left", left_on="flow", right_on="values")
        .merge(df_year, how="left", left_on="year", right_on="values")
    )

    df_trades_clean["id_trades"] = df_trades_clean.index + 1

    df_trades_final = df_trades_clean[
        ["id_trades", "trade_usd", "kg", "quantity", "id_code", "id_country", "id_quantity", "id_flow", "id_year"]
    ].copy()

    df_countries = df_countries[["id_country", "alpha-3", "country", "region", "sub-region"]]

    df_codes = df_codes[["id_code", "clean_code", "Description", "parent_description"]]

    return {
        "trades": df_trades_final,
        "countries": df_countries,
        "codes": df_codes,
        "quantity": df_quantity,
        "flow": df_flow,
        "year": df_year,
    }


def load(dimentions: Dict[str, DataFrame]):
    dimentions["trades"].to_csv("target/trades.csv", index=False, sep="|")

    dimentions["countries"].to_csv("target/countries.csv", index=False, sep="|")

    dimentions["codes"].to_csv("target/codes.csv", index=False, sep="|")

    dimentions["quantity"].to_csv("target/quantity.csv", index=False, sep="|")

    dimentions["flow"].to_csv("target/flow.csv", index=False, sep="|")

    dimentions["year"].to_csv("target/year.csv", index=False, sep="|")


def main():
    data = extract()
    dimentions = transform(data)
    load(dimentions)


if __name__ == "__main__":
    main()
