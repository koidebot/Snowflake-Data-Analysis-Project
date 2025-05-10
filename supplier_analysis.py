# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit, dayofweek, sum, count, datediff, max, to_date
import pandas as pd
import altair as alt

# Write directly to the app
st.set_page_config(layout="wide")
st.title("Supplier Analysis App")

session = get_active_session()

tab = st.radio('Select a Tab', ['Sales Forecast', 'Sales Analysis Visualization'])

#import data
df_customer = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER")
df_lineitem = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM")
df_nation = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION")
df_orders = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS")
df_part = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART")
df_partsupp = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PARTSUPP")
df_region = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION")
df_supplier = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER")

if tab == "Sales Analysis Visualization":
    col_cust, col_sup = st.columns(2)
    df_partsupp_b = df_partsupp.select("PS_PARTKEY", "PS_SUPPLYCOST")
    df_part_b = df_part.select("P_PARTKEY", "P_BRAND", "P_RETAILPRICE")
    
    df_b = df_part_b.join(df_partsupp_b, df_part_b["P_PARTKEY"] == df_partsupp_b["PS_PARTKEY"])
    df_b = df_b.drop("PS_PARTKEY")
    
    df_b = df_b.with_column("PROFIT_PER_PART", col("P_RETAILPRICE") - col("PS_SUPPLYCOST"))
    df_b = df_b.with_column("PROFIT_MARGIN_PER_PART", col("PROFIT_PER_PART")/col("P_RETAILPRICE"))

    df_profitable_brand = df_b.group_by("P_BRAND").agg(
        sum(col("PROFIT_PER_PART")).alias("GROSS_NET_PROFIT"),  # Total profit
        max(col("PROFIT_PER_PART")).alias("MAX_PROFIT"),  # Max profit per part
        count("*").alias("TOTAL_ORDERS"),  # Total number of orders
        (sum(col("PROFIT_PER_PART")) / sum(col("P_RETAILPRICE"))).cast("float").alias("AVG_PROFIT_MARGIN")  # Corrected formula
    )
    df_profitable_brand = df_profitable_brand.with_column("AVG_PROFIT", col("GROSS_NET_PROFIT")/col("TOTAL_ORDERS").cast("float"))

    most_profit = df_profitable_brand.sort(col("GROSS_NET_PROFIT").desc()).limit(10)
    most_profit = most_profit.to_pandas()
    
    with col_cust:
    #customer analysis
        df_customer_c = df_customer.drop(["C_NAME", "C_PHONE", "C_ADDRESS", "C_ACCTBAL", "C_COMMENT", "C_MKTSEGMENT", "C_NATIONKEY"])
        df_orders_c = df_orders.drop(["O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"])
        df_lineitem_c = df_lineitem.drop(["L_COMMENT", "L_SHIPINSTRUCT", "L_TAX", "L_LINESTATUS", "L_LINENUMBER", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPMODE"])
        
        df_c = df_lineitem_c.join(df_orders_c, (df_lineitem_c["L_ORDERKEY"] == df_orders_c["O_ORDERKEY"]))
        df_c = df_c.join(df_customer_c, (df_c["O_CUSTKEY"] == df_customer_c["C_CUSTKEY"]))
        df_c = df_c.drop("O_CUSTKEY", "L_ORDERKEY")
        df_c = df_c.with_column("RETURNED", (col("L_RETURNFLAG") != "N").cast("integer"))
        df_c = df_c.with_column("DISCOUNTED", (col("L_DISCOUNT") > 0).cast("integer"))
        
        #customers with high return rate
        df_c = df_c.drop("L_RETURNFLAG")
        df_returnrate = df_c.group_by("C_CUSTKEY").agg(sum(col("RETURNED")).alias("TOTAL_RETURNS"),count("*").alias("TOTAL_PURCHASES")).select("C_CUSTKEY", "TOTAL_RETURNS", "TOTAL_PURCHASES")
        df_returnrate = df_returnrate.with_column("RETURN_RATE", col("TOTAL_RETURNS")/col("TOTAL_PURCHASES").cast("float"))
        
        # purchases = st.number_input("Filter on minimum purchases", min_value=0, step=1)
        # return_rate = st.number_input("Filter on return rate", min_value=0.0, max_value=1.0, step=0.01)
        
        #customers with high discount rates
        df_tmp = df_c.groupBy("C_CUSTKEY").agg(
            sum(col("DISCOUNTED")).alias("DISCOUNTED_PURCHASES"),
            sum(col("L_DISCOUNT")).alias("TOTAL_DISCOUNT"),
            count("*").alias("TOTAL_PURCHASES")
        ).select("C_CUSTKEY", "DISCOUNTED_PURCHASES", "TOTAL_DISCOUNT", "TOTAL_PURCHASES")
        
        df_disocountrate = df_tmp.with_column("AVG_DISCOUNT_RATE", 100*col("TOTAL_DISCOUNT")/(col("TOTAL_PURCHASES").cast("float")))
        df_disocountrate = df_disocountrate.drop(["TOTAL_DISCOUNT"])
    
        #UI
        st.write("Top and Bottom Return Rates")
        # df_returnrate_high = df_returnrate.sort(col("RETURN_RATE").desc()).limit(3000)
        # df_returnrate_low = df_returnrate.sort(col("RETURN_RATE").asc()).limit(3000)
        # df_returnrate = df_returnrate_high.union(df_returnrate_low)
        df_returnrate = df_returnrate.to_pandas()
        scatter_chart = alt.Chart(df_returnrate).mark_circle(size=5).encode(
            x=alt.X("TOTAL_PURCHASES:Q", title="Total Purchases"),
            y=alt.Y("RETURN_RATE:Q", title="Return Rate"),
            tooltip=["C_CUSTKEY", "TOTAL_PURCHASES", "RETURN_RATE"],  # Show details on hover
            color=alt.Color("RETURN_RATE:Q", scale=alt.Scale(scheme="redblue")),
        ).properties(
            width=500,
            height=400).interactive()
        
    
        # Display in Streamlit
        st.altair_chart(scatter_chart)
        topdiscounters = df_disocountrate.sort(col("AVG_DISCOUNT_RATE").desc()).limit(30)
        st.write("Customers with highest discount rates")
        st.bar_chart(topdiscounters, x="C_CUSTKEY", y="AVG_DISCOUNT_RATE", x_label="Customer", y_label="Average Discount Rate", width=1000)

        st.write("Brands with the Highest Net Profit")
        brand_chart1 = alt.Chart(most_profit).mark_bar().encode(
            x=alt.X("P_BRAND:N", title="Brand Name"), 
            y=alt.Y("GROSS_NET_PROFIT:Q", title="Net Profit", scale=alt.Scale(domain=[31000000, 33000000])), 
            tooltip = "TOTAL_ORDERS"
        ).properties(width=500, height=500).interactive()
    
        st.altair_chart(brand_chart1)

    with col_sup:
        #supplier analysis
        df_lineitem_s = df_lineitem.select(["L_PARTKEY", "L_SUPPKEY", "L_SHIPDATE", "L_COMMITDATE"])
        df_supplier_s = df_supplier.select(["S_SUPPKEY", "S_NAME"])
        
        df_s = df_lineitem_s.join(df_supplier_s, (df_lineitem_s["L_SUPPKEY"] == df_supplier_s["S_SUPPKEY"]))
        df_s = df_s.drop(["L_SUPPKEY", "PS_SUPPKEY"])
        df_s = df_s.with_column("DELIVERY_TIME", datediff("DAY", col1=col("L_COMMITDATE"), col2=col("L_SHIPDATE")))
        
        df_deliverytime = df_s.group_by("S_SUPPKEY", "S_NAME").agg(
            ((sum(col("DELIVERY_TIME"))/count("*"))).cast("FLOAT").alias("AVERAGE_DELIVERY_TIME"),
            max(col("DELIVERY_TIME")).alias("LONGEST_DELAY"),
            count(col("DELIVERY_TIME")).alias("TOTAL_ORDERS")
        )
        
        sup_chart_data = df_deliverytime.to_pandas()
        st.write("Average Delivery Time by Total Orders")
        chart1 = alt.Chart(sup_chart_data).mark_square().encode(
            x=alt.X("AVERAGE_DELIVERY_TIME:Q", title="Average Delivery Time"),
            y=alt.Y("TOTAL_ORDERS:Q", scale=alt.Scale(domain=[450, 750]), title="Total Orders"), 
            tooltip = "S_SUPPKEY",
            color=alt.Color('AVERAGE_DELIVERY_TIME:Q').scale(scheme="greenblue")
        ).properties(
            width=800, height=500
        ).interactive()
        st.altair_chart(chart1)
    
        st.write("Longest Delay Distribution")
        delay_count = df_deliverytime.group_by("LONGEST_DELAY").agg(count("S_SUPPKEY").alias("SUPPLIER_COUNT"))
        delay_count = delay_count.to_pandas()
        chart2 = alt.Chart(delay_count).mark_bar(stroke=None).encode(
            y=alt.Y("SUPPLIER_COUNT:Q"),
            x=alt.X("LONGEST_DELAY:Q")
        )
        st.altair_chart(chart2)

        st.write("Profit Margins vs Total Orders")
        brand_chart_data = df_profitable_brand.to_pandas()
        order_range = [brand_chart_data["TOTAL_ORDERS"].min(), brand_chart_data["TOTAL_ORDERS"].max()]
        margin_range = [brand_chart_data["AVG_PROFIT_MARGIN"].min(), brand_chart_data["AVG_PROFIT_MARGIN"].max()]
        brand_chart2 = alt.Chart(brand_chart_data).mark_circle(size=30).encode(
            x=alt.X("AVG_PROFIT_MARGIN:Q", title="Average Profit Margins", scale=alt.Scale(domain=margin_range)),
            y=alt.Y("TOTAL_ORDERS:Q", title="Total Orders", scale=alt.Scale(domain=order_range)), 
            color=alt.Color("AVG_PROFIT_MARGIN:Q", scale=alt.Scale(scheme="greenblue")),
            tooltip = "P_BRAND"
        ).properties(width=1000, height=500).interactive()
        st.altair_chart(brand_chart2)

if tab == "Sales Forecast":
    sales = session.table("SALES_DATA")

    sales = sales.with_column("Date", to_date(col("DATE"), "DD/MM/YYYY")).limit(300)
    
    sales.write.mode("overwrite").save_as_table("SALES")
    
    pd_df = sales.select("STORE", "DEPT", "WEEKLY_SALES", "DATE").toPandas()
    
    store = st.selectbox("Select a Store", pd_df["STORE"].unique())
    department = st.selectbox("Select a Department", pd_df[pd_df["STORE"] == store]["DEPT"].unique())
    weeks = st.number_input("How many weeks", step=1)

    if weeks:
        create_view_query = f"""
            CREATE OR REPLACE VIEW v1 AS 
            SELECT ARRAY_CONSTRUCT(store, dept) AS store_dept, date, weekly_sales
            FROM SALES where store={store} and dept={department};
        """
        
        session.sql(create_view_query).collect()
        
        create_model_query = """
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST model2(
                INPUT_DATA => TABLE(v1),
                SERIES_COLNAME => 'store_dept',
                TIMESTAMP_COLNAME => 'date',
                TARGET_COLNAME => 'weekly_sales'
            );
        """
        
        session.sql(create_model_query).collect()
        forecast_query = f"""
            CALL model2!FORECAST(FORECASTING_PERIODS => {weeks});
        """
        forecast_df = session.sql(forecast_query)
        
        forecast_df = session.table("FORECAST")
        sales = sales.drop("ISHOLIDAY")
        
        upper_df = forecast_df.select(
            col("SERIES")[0].alias("STORE"), 
            col("SERIES")[1].alias("DEPT"), 
            col("UPPER_BOUND").alias("WEEKLY_SALES"),
            col("TS").alias("DATE"),  
        )
        
        lower_df = forecast_df.select(
            col("SERIES")[0].alias("STORE"), 
            col("SERIES")[1].alias("DEPT"), 
            col("LOWER_BOUND").alias("WEEKLY_SALES"),
            col("TS").alias("DATE"),  
        )
        
        forecast_df = forecast_df.select(
            col("SERIES")[0].alias("STORE"), 
            col("SERIES")[1].alias("DEPT"), 
            col("FORECAST").alias("WEEKLY_SALES"),
            col("TS").alias("DATE"),  
        )
    
        upper_df = sales.union(upper_df).to_pandas()
        lower_df = sales.union(lower_df).to_pandas()
        forecast_df = sales.union(forecast_df).to_pandas()
        
        upper_df["Type"] = "Upper Bound"
        lower_df["Type"] = "Lower Bound"
        forecast_df["Type"] = "Forecast"
        sales_df = sales.to_pandas() 
        sales_df["Type"] = "Actual Sales"
        
        combined_df = pd.concat([sales_df, forecast_df, upper_df, lower_df])
        combined_df["DATE"] = pd.to_datetime(combined_df["DATE"])
        chart = alt.Chart(combined_df).mark_line().encode(
            x="DATE:T",
            y="WEEKLY_SALES:Q",
            color="Type:N",  
            tooltip=["DATE:T", "WEEKLY_SALES:Q", "Type:N"]
        ).properties(
            title="Sales Forecast with Confidence Bounds"
        )
        
        st.altair_chart(chart, use_container_width=True)
    

        
        
