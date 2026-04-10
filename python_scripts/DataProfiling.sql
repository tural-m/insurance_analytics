import snowflake.snowpark as sp
import pandas as pd

def profile(name, df):
    
    report = f"===================================\n"
    report += f"Table: {name}\n"
    report += f"Shape: {df.shape}\n"
    report += f"\n"
    report += f"Nulls:\n{df.isnull().sum()[df.isnull().sum()>0]}\n"
    report += f"\nDtypes:\n{df.dtypes}\n"
    report += f"Duplicates:{df.duplicated().sum()}\n"
    report += f"\nCategoricals Columns:\n"
    cat_cols = df.select_dtypes(include="object").columns
    for col in cat_cols:
        if df[col].nunique() <= 10:
            report += f"{col}\n"
    report += f"===================================\n"

    return report

def main(session: sp.Session):

    df_customer = session.table("INSURANCE_DB.BRONZE.DIM_CUSTOMER").to_pandas()
    df_driving = session.table("INSURANCE_DB.BRONZE.DIM_DRIVING").to_pandas()
    df_health = session.table("INSURANCE_DB.BRONZE.DIM_HEALTH").to_pandas()
    df_premium = session.table("INSURANCE_DB.BRONZE.FACT_RISK_PREMIUM").to_pandas()

    report = profile("DIM_CUSTOMER", df_customer)
    report += profile("DIM_DRIVING", df_driving)
    report += profile("DIM_HEALTH", df_health)
    report += profile("FACT_RISK_PREMIUM", df_premium)

    return report