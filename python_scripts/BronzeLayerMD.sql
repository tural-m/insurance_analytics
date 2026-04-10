import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    session.sql("CREATE DATABASE IF NOT EXISTS INSURANCE_DB").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS INSURANCE_DB.BRONZE").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS INSURANCE_DB.SILVER").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS INSURANCE_DB.GOLD").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS INSURANCE_DB.GOLD_DETAIL").collect()

    create_tables(session)
    return 'Done'
    
def create_tables(session):
    session.sql("""
        CREATE TABLE IF NOT EXISTS INSURANCE_DB.BRONZE.DIM_CUSTOMER(
            CustomerID VARCHAR,
            Age VARCHAR,
            AgeBand VARCHAR,
            AgeBandSort NUMBER,
            Gender VARCHAR,
            IncomeBand VARCHAR,
            Region VARCHAR,
            City VARCHAR,
            MaritalStatus VARCHAR,
            EducationLevel VARCHAR
        )
    """).collect()
    
    session.sql("""
        CREATE TABLE IF NOT EXISTS INSURANCE_DB.BRONZE.DIM_DRIVING(
            CustomerID VARCHAR,
            DrivingExperienceYears VARCHAR,
            DrivingExperienceBand VARCHAR,
            DrivingExperienceSort NUMBER,
            VehicleType VARCHAR,
            PreviousAccidents NUMBER
        )
    """).collect()

    session.sql("""
        CREATE TABLE IF NOT EXISTS INSURANCE_DB.BRONZE.DIM_HEALTH(
            CustomerID VARCHAR,
            Smoker NUMBER,
            ChronicCondition NUMBER,
            BMICategory VARCHAR,
            AlcoholUse VARCHAR,
            ExerciseFrequency VARCHAR
        )
    """).collect()

    session.sql("""
        CREATE TABLE IF NOT EXISTS INSURANCE_DB.BRONZE.FACT_RISK_PREMIUM(
            PolicyID VARCHAR,
            CustomerID VARCHAR,
            RiskScore VARCHAR,
            RiskCategory VARCHAR,
            AnnualPremium VARCHAR,
            ClaimAmount NUMBER,
            ClaimsCount NUMBER,
            PolicyYear NUMBER
        )
    """).collect()

    return "Tables are created."
