import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, dayofweek, sum, count, datediff, max, listagg, concat, parse_json, sql_expr, regexp_replace
import os

# Set page configuration
st.set_page_config(page_title="AI Amazon Product Manager", layout="wide")

# Function to create Snowflake connection
def create_snowflake_connection():
    try:
        connection_parameters = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "password": st.secrets["snowflake"]["password"],
            "role": st.secrets["snowflake"]["role"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "database": st.secrets["snowflake"]["database"],
            "schema": st.secrets["snowflake"]["schema"]
        }
    except Exception:
        # Fallback to environment variables if secrets are not available
        connection_parameters = {
            "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
            "user": os.environ.get("SNOWFLAKE_USER"),
            "password": os.environ.get("SNOWFLAKE_PASSWORD"),
            "role": os.environ.get("SNOWFLAKE_ROLE"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
            "database": os.environ.get("SNOWFLAKE_DATABASE"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA")
        }
    
    # Create Snowflake session
    session = Session.builder.configs(connection_parameters).create()
    return session

# Connect to Snowflake
@st.cache_resource
def get_snowflake_session():
    return create_snowflake_connection()

# App title
st.title("AI Amazon Product Manager")

# Initialize session state
if "query" not in st.session_state:
    st.session_state["query"] = None

# Function to load data
@st.cache_data(ttl=3600)
def load_data(_session):
    meta = _session.table("BEAUTY_PRODUCTS")
    reviews = _session.table("BEAUTY_REVIEWS")
    
    df_tmp = reviews.join(meta, meta["Parent_ID_META"] == reviews["Parent_ID_REV"])
    df_tmp = df_tmp.drop("Parent_ID_META")
    df_tmp = df_tmp.limit(10000)
    
    df = df_tmp.group_by("Parent_ID_REV").agg(
        listagg(col("REVIEW"), "\n").alias("Reviews"),
        sum(col("NUM_RATINGS")).alias("Total Ratings"), 
        (sum(col("AVG_RATING") * col("NUM_RATINGS")) / sum(col("NUM_RATINGS"))).alias("Average Rating"), 
        (sum(col("Sentiment") * col("NUM_RATINGS")) / sum(col("NUM_RATINGS"))).alias("Sentiment")
    ).select(["Parent_ID_REV", "Reviews", "Total Ratings", "Average Rating", "Sentiment"])
    
    df = df.with_column("Combined", concat(lit("ID: "), col("PARENT_ID_REV"), lit(": "), col("REVIEWS")))
    
    # Save data to Snowflake table
    df.write.mode("overwrite").save_as_table("ALL_PRODUCTS")
    
    # Convert to pandas
    pd_df = df.select("PARENT_ID_REV", "REVIEWS", "Total Ratings", "Average Rating", "SENTIMENT").toPandas()
    return pd_df

# Function to update selection
def update_selection():
    st.session_state["query"] = None

# Function to query Snowflake Cortex
def get_ai_summary(_session, prompt, reviews):
    try:
        sql_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'snowflake-arctic',
                CONCAT('[INST] ', '{prompt}', ' ', '{reviews}', ' [/INST]')
            ) AS summary
        """
        summary_df = _session.sql(sql_query).collect()
        if summary_df:
            return summary_df[0]["SUMMARY"]
        return "No summary available."
    except Exception as e:
        return f"Error generating summary: {str(e)}"

# Main app
try:
    # Get Snowflake session
    session = get_snowflake_session()
    
    # Load data
    with st.spinner("Loading data..."):
        pd_df = load_data(_session=session)
    
    # Product selection
    product_id = st.selectbox("Select a Product", pd_df["PARENT_ID_REV"].dropna().unique(), on_change=update_selection)
    
    # Product stats
    st.write(f'⭐️ Average Rating: {pd_df[pd_df["PARENT_ID_REV"] == product_id]["Average Rating"].mean():.2f}/5')
    
    # Sentiment display
    col1, col2 = st.columns(spec=[0.2, 0.8])
    with col1:
        st.write("Sentiment Score:")
    
    sentiment = pd_df[pd_df["PARENT_ID_REV"] == product_id]["SENTIMENT"].mean()
    progress_value = int((sentiment + 1) * 50)  # Convert sentiment (-1 to 1) to percentage (0 to 100)
    
    with col2:
        st.progress(progress_value)
    
    # Get reviews for selected product
    reviews = pd_df[pd_df["PARENT_ID_REV"] == product_id]["REVIEWS"].dropna().iloc[0] if not pd_df[pd_df["PARENT_ID_REV"] == product_id]["REVIEWS"].dropna().empty else ""
    
    # Sample of reviews
    with st.expander("View sample of reviews"):
        st.write(reviews[:1000] + "..." if len(reviews) > 1000 else reviews)
    
    # AI query
    st.subheader("Ask about this product")
    q = st.text_input("Place your question here", key="query")
    
    if q:
        with st.spinner("Analyzing reviews..."):
            prompt = f'You are a product manager presenting customer feedback, and were asked this by your client: {q}. Provide a summary of reviews and feedback on the query you were given.'
            summary_text = get_ai_summary(_session=session, prompt=prompt, reviews=reviews)
            
            st.subheader("AI Analysis")
            st.write(summary_text)
    

except Exception as e:
    st.error(f"Application error: {str(e)}")
