import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, dayofweek, sum, count, datediff, max, to_date

# Configure page
st.set_page_config(page_title="Supplier Analysis App", layout="wide")

# Establish Snowflake connection via Streamlit secrets
@st.cache_resource
def get_snowflake_session():
    creds = st.secrets["snowflake"]
    params = {
        "account":   creds["account"],
        "user":      creds["user"],
        "password":  creds["password"],
        "role":      creds["role"],
        "warehouse": creds["warehouse"],
        "database":  creds["database"],
        "schema":    creds["schema"],
    }
    return Session.builder.configs(params).create()

# Initialize session
try:
    session = get_snowflake_session()
except Exception as e:
    st.error(f"Failed to connect to Snowflake: {e}")
    st.stop()

# App UI
tab = st.radio("Select a Tab", ["Sales Analysis Visualization", "Sales Forecast"])

if tab == "Sales Analysis Visualization":
    # Load tables
    try:
        df_customer = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER")
        df_lineitem = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM")
        df_partsupp = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PARTSUPP")
        df_part     = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART")
        df_supplier = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER")
        df_orders   = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS")
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.stop()

    # Two-column layout
    col_cust, col_sup = st.columns(2)

    # ========== Sales Analysis ==========
    with col_cust:
        # Customer return and discount analysis
        df_cust = (
            df_lineitem
            .join(df_orders, df_lineitem["L_ORDERKEY"] == df_orders["O_ORDERKEY"])
            .join(df_customer, df_orders["O_CUSTKEY"] == df_customer["C_CUSTKEY"])
            .with_column("RETURNED", (col("L_RETURNFLAG") != "N").cast("integer"))
            .with_column("DISCOUNTED", (col("L_DISCOUNT") > 0).cast("integer"))
        )
        # Return rates
        df_return = (
            df_cust.group_by("C_CUSTKEY")
                   .agg(
                       sum(col("RETURNED")).alias("TOTAL_RETURNS"),
                       count("*").alias("TOTAL_PURCHASES")
                   )
                   .with_column("RETURN_RATE", col("TOTAL_RETURNS") / col("TOTAL_PURCHASES").cast("float"))
                   .to_pandas()
        )
        scatter = alt.Chart(df_return).mark_circle(size=50).encode(
            x="TOTAL_PURCHASES:Q", y="RETURN_RATE:Q",
            tooltip=["C_CUSTKEY", "TOTAL_PURCHASES", "RETURN_RATE"],
            color=alt.Color("RETURN_RATE:Q", scale=alt.Scale(scheme="redblue"))
        ).properties(title="Return Rate vs Purchases").interactive()
        st.altair_chart(scatter, use_container_width=True)

        # Discount analysis
        df_disc = (
            df_cust.group_by("C_CUSTKEY")
                   .agg(
                       sum(col("DISCOUNTED")).alias("DISCOUNTED_PURCHASES"),
                       sum(col("L_DISCOUNT")).alias("TOTAL_DISCOUNT"),
                       count("*").alias("TOTAL_PURCHASES")
                   )
                   .with_column("AVG_DISCOUNT_RATE", 100 * col("TOTAL_DISCOUNT") / col("TOTAL_PURCHASES").cast("float"))
                   .select("C_CUSTKEY", "AVG_DISCOUNT_RATE")
                   .to_pandas()
        )
        st.write("### Top Discount Rates")
        st.bar_chart(df_disc.sort_values("AVG_DISCOUNT_RATE", ascending=False).head(20), x="C_CUSTKEY", y="AVG_DISCOUNT_RATE")

    # ========== Brand Profit Analysis ==========
    df_profit = (
        df_part.join(df_partsupp, df_part["P_PARTKEY"] == df_partsupp["PS_PARTKEY"])  # join
               .with_column("PROFIT", col("P_RETAILPRICE") - col("PS_SUPPLYCOST"))
               .group_by("P_BRAND")
               .agg(
                   sum(col("PROFIT")).alias("GROSS_PROFIT"),
                   max(col("PROFIT")).alias("MAX_PROFIT"),
                   count("*").alias("TOTAL_ORDERS"),
                   (sum(col("PROFIT")) / sum(col("P_RETAILPRICE"))).cast("float").alias("AVG_MARGIN")
               )
               .to_pandas()
    )
    st.write("### Top Brands by Net Profit")
    bar = alt.Chart(df_profit.sort_values("GROSS_PROFIT", ascending=False).head(10)).mark_bar().encode(
        x=alt.X("P_BRAND:N", title="Brand"),
        y=alt.Y("GROSS_PROFIT:Q", title="Net Profit"),
        tooltip=["TOTAL_ORDERS", "AVG_MARGIN"]
    ).interactive()
    st.altair_chart(bar, use_container_width=True)

    # ========== Supplier Delivery Analysis ==========
    with col_sup:
        df_del = (
            df_lineitem.select("L_SUPPKEY", "L_SHIPDATE", "L_COMMITDATE")
                       .join(df_supplier, df_lineitem["L_SUPPKEY"] == df_supplier["S_SUPPKEY"] )
                       .with_column("DELIVERY_DAYS", datediff("day", col("L_SHIPDATE"), col("L_COMMITDATE")))
        )
        df_delagg = (
            df_del.group_by("S_SUPPKEY", "S_NAME")
                  .agg(
                      (sum(col("DELIVERY_DAYS")) / count("*")).alias("AVG_DELAY"),
                      max(col("DELIVERY_DAYS")).alias("MAX_DELAY"),
                      count("*").alias("NUM_ORDERS")
                  )
                  .to_pandas()
        )
        st.write("### Avg Delivery Time vs Orders")
        chart = alt.Chart(df_delagg).mark_circle().encode(
            x="AVG_DELAY:Q", y="NUM_ORDERS:Q",
            tooltip=["S_NAME", "AVG_DELAY", "NUM_ORDERS"],
            color=alt.Color("AVG_DELAY:Q", scale=alt.Scale(scheme="greenblue"))
        ).interactive()
        st.altair_chart(chart, use_container_width=True)

elif tab == "Sales Forecast":
    # Check if SALES_DATA table exists
    sales_exists = False
    try:
        sales_tables = session.sql("SHOW TABLES LIKE 'SALES_DATA'").collect()
        sales_exists = len(sales_tables) > 0
    except:
        pass
    
    if not sales_exists:
        st.error("SALES_DATA table not found. Please create this table in your Snowflake database.")
        st.stop()
    
    # Load and prepare
    try:
        sales = session.table("SALES_DATA").with_column("DATE", to_date(col("DATE"), "DD/MM/YYYY")).limit(500)
        pd_sales = sales.to_pandas()
    except Exception as e:
        st.error(f"Error loading sales data: {e}")
        st.stop()

    if pd_sales.empty:
        st.warning("No sales data available.")
        st.stop()

    store     = st.selectbox("Store", pd_sales["STORE"].unique())
    department= st.selectbox("Dept", pd_sales[pd_sales["STORE"] == store]["DEPT"].unique())
    weeks     = st.number_input("Forecast Weeks", min_value=1, value=4, step=1)

    if weeks:
        try:
            # Build view and train forecast model
            session.sql(f"""
                CREATE OR REPLACE VIEW v_sales AS
                SELECT ARRAY_CONSTRUCT(store, dept) AS series, date, weekly_sales
                FROM SALES_DATA
                WHERE store = '{store}' AND dept = '{department}';
            """).collect()
            
            session.sql(
                """
                CREATE OR REPLACE FUNCTION forecast_model AS \
                SELECT * FROM SNOWFLAKE.ML.FORECAST(
                    INPUT_DATA => TABLE(v_sales),
                    SERIES_COLNAME => 'series',
                    TIMESTAMP_COLNAME => 'date',
                    TARGET_COLNAME => 'weekly_sales'
                )
                """
            ).collect()
            
            session.sql(f"CALL forecast_model(FORECASTING_PERIODS => {weeks})").collect()

            # Fetch results
            fc = session.table("FORECAST").to_pandas()
            actual = pd_sales.assign(Type="Actual")
            forecast = fc.rename(columns={"FORECAST": "weekly_sales", "TS": "date"}).assign(Type="Forecast")
            data = pd.concat([actual, forecast])
            data["date"] = pd.to_datetime(data["date"])

            line = alt.Chart(data).mark_line().encode(
                x="date:T", y="weekly_sales:Q", color="Type:N",
                tooltip=["date", "weekly_sales", "Type"]
            ).properties(title="Sales Forecast")
            st.altair_chart(line, use_container_width=True)
        except Exception as e:
            st.error(f"Error generating forecast: {e}")
            st.info("Make sure your Snowflake account has Snowpark ML capabilities enabled.")
