import snowflake.snowpark as sp
import pandas as pd

def main(session: sp.Session):
    session.sql("USE SCHEMA INSURANCE_DB.GOLD_DETAIL").collect()

    df_customer = session.table("INSURANCE_DB.SILVER.DIM_CUSTOMER").to_pandas()
    df_driving = session.table("INSURANCE_DB.SILVER.DIM_DRIVING").to_pandas()
    df_health = session.table("INSURANCE_DB.SILVER.DIM_HEALTH").to_pandas()
    df_premium = session.table("INSURANCE_DB.SILVER.FACT_RISK_PREMIUM").to_pandas()

    df = df_premium.merge(df_customer, on="CUSTOMERID", how="left")
    df = df.merge(df_driving, on="CUSTOMERID", how="left")
    df = df.merge(df_health, on="CUSTOMERID", how="left")

    session.create_dataframe(df).write.save_as_table("INSURANCE_DB.GOLD_DETAIL.INSURANCE_DETAIL", mode="overwrite")

    return session.create_dataframe(df)