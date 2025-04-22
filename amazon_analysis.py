# Import python packages
import streamlit as st
import pandas as pd
import altair as alt

# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit, dayofweek, sum, count, datediff, max, listagg, concat, parse_json, sql_expr, regexp_replace

session = get_active_session()

meta = session.table("BEAUTY_PRODUCTS")
reviews = session.table("BEAUTY_REVIEWS")

st.title("Amazon Products Manager")

df_tmp = reviews.join(meta, meta["Parent_ID_META"] == reviews["Parent_ID_REV"])
df_tmp.drop("Parent_ID_META")

df_tmp = df_tmp.limit(10000)
df = df_tmp.group_by("Parent_ID_REV").agg(
    listagg(col("REVIEW"), "\n").alias("Reviews"),
    sum(col("NUM_RATINGS")).alias("Total Ratings"), 
    (sum(col("AVG_RATING") * col("NUM_RATINGS")) / sum(col("NUM_RATINGS"))).alias("Average Rating"), 
    (sum(col("Sentiment") * col("NUM_RATINGS")) / sum(col("NUM_RATINGS"))).alias("Sentiment")

).select(["Parent_ID_REV", "Reviews", "Total Ratings", "Average Rating", "Sentiment"])

df = df.with_column("Combined", concat(lit("ID: "), col("PARENT_ID_REV"), lit(": "), col("REVIEWS")))

df.write.mode("overwrite").save_as_table("ALL_PRODUCTS")
pd_df = df.select("PARENT_ID_REV", "REVIEWS", "Total Ratings", "Average Rating", "SENTIMENT").toPandas()
q = None
def update_selection():
    st.session_state["query"] = None
    
product_id = st.selectbox("Select a Product", pd_df["PARENT_ID_REV"].dropna().unique(), on_change=update_selection)

st.write(f'⭐️ Average Rating: {pd_df[pd_df["PARENT_ID_REV"] == product_id]["Average Rating"].mean()}/5')

col1, col2= st.columns(spec=[0.2, 0.8])
with col1:
    st.write("Sentiment Score:")
sentiment = pd_df[pd_df["PARENT_ID_REV"] == product_id]["SENTIMENT"].mean()
progress_value = int((sentiment + 1) * 50)
with col2:
    st.progress(progress_value)

reviews = pd_df[pd_df["PARENT_ID_REV"] == product_id]["REVIEWS"].dropna()

q = st.text_input("Place your question here", key="query")

if q:
    prompt = f'You are a product manager presenting customer feedback, and were asked this by your client: {q}. Provide a summary of reviews and feedback on the query you were given.'
    
    sql_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'snowflake-arctic',
            CONCAT('[INST] ', '{prompt}', ' ', '{reviews}', ' [/INST]')
        ) AS summary
    """
    
    summary_df = session.sql(sql_query).collect()
    
    if summary_df:
        summary_text = summary_df[0]["SUMMARY"]
        st.write(summary_text)

    
