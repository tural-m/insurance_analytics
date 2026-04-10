import snowflake.snowpark as sp
import pandas as pd

def clean_customer(session):
    df = session.table("INSURANCE_DB.BRONZE.DIM_CUSTOMER").to_pandas()

    df["AGE"] = df["AGE"].astype(float)
    df = df.dropna(subset=["AGE"])

    str_col = df.select_dtypes(include="object").columns
    for col in str_col:
        df[col] = df[col].str.strip()

    cat_cols = df.select_dtypes(include="object").columns
    for col in cat_cols:
        if df[col].nunique() <= 10:
            df[col] = df[col].str.title()
    
    return df

def clean_driving(session):
    df = session.table("INSURANCE_DB.BRONZE.DIM_DRIVING").to_pandas()

    df["DRIVINGEXPERIENCEYEARS"] = df["DRIVINGEXPERIENCEYEARS"].astype(float)
    df = df.dropna(subset=["DRIVINGEXPERIENCEYEARS"])

    str_col = df.select_dtypes(include="object").columns
    for col in str_col:
        df[col] = df[col].str.strip()

    cat_cols = df.select_dtypes(include="object").columns
    for col in cat_cols:
        if df[col].nunique() <= 10:
            df[col] = df[col].str.title()
    
    return df

def clean_health(session):
    df = session.table("INSURANCE_DB.BRONZE.DIM_HEALTH").to_pandas()

    df = df.dropna(subset=["BMICATEGORY"])

    str_col = df.select_dtypes(include="object").columns
    for col in str_col:
        df[col] = df[col].str.strip()

    cat_cols = df.select_dtypes(include="object").columns
    for col in cat_cols:
        if df[col].nunique() <= 10:
            df[col] = df[col].str.title()
    
    return df

def clean_premium(session):
    df = session.table("INSURANCE_DB.BRONZE.FACT_RISK_PREMIUM").to_pandas()

    df[["RISKSCORE", "ANNUALPREMIUM"]] = df[["RISKSCORE", "ANNUALPREMIUM"]].astype(float)
    df = df.dropna(subset=["RISKSCORE", "ANNUALPREMIUM"])

    str_col = df.select_dtypes(include="object").columns
    for col in str_col:
        df[col] = df[col].str.strip()
        
    df = df.drop_duplicates(subset=["POLICYID"])
    
    cat_cols = df.select_dtypes(include="object").columns
    for col in cat_cols:
        if df[col].nunique() <= 10:
            df[col] = df[col].str.title()
            
    df["CLAIMAMOUNT"] = df["CLAIMAMOUNT"].where(df["CLAIMSCOUNT"] != 0, 0)
    
    df = df.merge(session.table("INSURANCE_DB.BRONZE.DIM_CUSTOMER")[["CUSTOMERID"]].to_pandas(), on="CUSTOMERID", how="inner")
    
    return df


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
    df_customer = clean_customer(session)
    df_driving = clean_driving(session)
    df_health = clean_health(session)
    df_premium = clean_premium(session)

    session.sql("USE SCHEMA INSURANCE_DB.SILVER").collect()
    
    session.create_dataframe(df_customer).write.save_as_table(
        "INSURANCE_DB.SILVER.DIM_CUSTOMER",
        mode="overwrite"
    )
    session.create_dataframe(df_driving).write.save_as_table(
        "INSURANCE_DB.SILVER.DIM_DRIVING",
        mode="overwrite"
    )
    session.create_dataframe(df_health).write.save_as_table(
        "INSURANCE_DB.SILVER.DIM_HEALTH",
        mode="overwrite"
    )
    session.create_dataframe(df_premium).write.save_as_table(
        "INSURANCE_DB.SILVER.FACT_RISK_PREMIUM",
        mode="overwrite"
    )
    
    report  = profile("DIM_CUSTOMER", df_customer)
    report += profile("DIM_DRIVING",  df_driving)
    report += profile("DIM_HEALTH",   df_health)
    report += profile("FACT_PREMIUM", df_premium)
        
    return report