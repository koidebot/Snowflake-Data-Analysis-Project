import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, dayofweek, sum, count, datediff, max, listagg, concat, parse_json, sql_expr, regexp_replace
import os

# Set page configuration
st.set_page_config(page_title="Amazon Products Manager", layout="wide", initial_sidebar_state="expanded")

# Function to create Snowflake connection
def create_snowflake_connection():
    # Get connection parameters from environment variables or Streamlit secrets
    # For local testing, you can use st.secrets
    # For deployment, set these as environment variables in Streamlit Cloud
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
st.title("Amazon Products Manager")

# Initialize session state
if "query" not in st.session_state:
    st.session_state["query"] = None

# Function to load data
@st.cache_data(ttl=3600)
def load_data(_session):
    try:
        # Get connection info for debugging
        connection_info = _session.get_current_account()
        current_db = _session.get_current_database()
        current_schema = _session.get_current_schema()
        current_role = _session.get_current_role()
        
        st.sidebar.write("Connected to Snowflake:")
        st.sidebar.write(f"- Account: {connection_info}")
        st.sidebar.write(f"- Database: {current_db}")
        st.sidebar.write(f"- Schema: {current_schema}")
        st.sidebar.write(f"- Role: {current_role}")
        
        # Use fully qualified table names
        database = current_db
        schema = current_schema
        
        # Option to select different schema/database
        with st.sidebar.expander("Change Database/Schema"):
            alt_db = st.text_input("Database", value=current_db)
            alt_schema = st.text_input("Schema", value=current_schema)
            if st.button("Use These Settings"):
                database = alt_db
                schema = alt_schema
                _session.use_database(database)
                _session.use_schema(schema)
        
        # Try to list tables to help debug
        try:
            tables = _session.sql(f"SHOW TABLES IN {database}.{schema}").collect()
            with st.sidebar.expander("Available Tables"):
                for table in tables:
                    st.write(f"- {table['name']}")
        except Exception as e:
            st.sidebar.warning(f"Could not list tables: {str(e)}")
        
        # Default table names
        meta_table = "BEAUTY_PRODUCTS"
        reviews_table = "BEAUTY_REVIEWS"
        
        # Add option to enter custom table names
        with st.sidebar.expander("Custom Table Names"):
            custom_meta_table = st.text_input("Products Table Name", value=meta_table)
            custom_reviews_table = st.text_input("Reviews Table Name", value=reviews_table)
            if st.button("Use These Tables"):
                meta_table = custom_meta_table
                reviews_table = custom_reviews_table
        
        # Try to access tables with selected names
        try:
            meta = _session.table(f"{database}.{schema}.{meta_table}")
            reviews = _session.table(f"{database}.{schema}.{reviews_table}")
            
            # Display sample data for debugging
            with st.sidebar.expander("Sample Data"):
                st.write("Products Table:")
                try:
                    meta_sample = meta.limit(5).to_pandas()
                    st.dataframe(meta_sample)
                except Exception as e:
                    st.write(f"Error displaying Products sample: {str(e)}")
                    
                st.write("Reviews Table:")
                try:
                    reviews_sample = reviews.limit(5).to_pandas()
                    st.dataframe(reviews_sample)
                except Exception as e:
                    st.write(f"Error displaying Reviews sample: {str(e)}")
        except Exception as e:
            st.error(f"Error accessing tables: {str(e)}")
            return pd.DataFrame()
        
        # Check column names
        meta_cols = meta.columns
        reviews_cols = reviews.columns
        
        with st.sidebar.expander("Column Names"):
            st.write("Products Table Columns:")
            st.write(meta_cols)
            st.write("Reviews Table Columns:")
            st.write(reviews_cols)
        
        # Check if required columns exist
        parent_id_meta = "Parent_ID_META" if "Parent_ID_META" in meta_cols else "PARENT_ID_META"
        parent_id_rev = "Parent_ID_REV" if "Parent_ID_REV" in reviews_cols else "PARENT_ID_REV"
        review_col = "REVIEW" if "REVIEW" in reviews_cols else "Review"
        num_ratings_col = "NUM_RATINGS" if "NUM_RATINGS" in reviews_cols else "Num_Ratings"
        avg_rating_col = "AVG_RATING" if "AVG_RATING" in reviews_cols else "Avg_Rating"
        sentiment_col = "Sentiment" if "Sentiment" in reviews_cols else "SENTIMENT"
        
        # Perform join with appropriate column names
        try:
            df_tmp = reviews.join(meta, meta[parent_id_meta] == reviews[parent_id_rev])
            
            # Drop redundant column if it exists
            if parent_id_meta in df_tmp.columns:
                df_tmp = df_tmp.drop(parent_id_meta)
                
            df_tmp = df_tmp.limit(10000)
            
            # Dynamic column references for aggregation
            df = df_tmp.group_by(parent_id_rev).agg(
                listagg(col(review_col), "\n").alias("Reviews"),
                sum(col(num_ratings_col)).alias("Total Ratings"), 
                (sum(col(avg_rating_col) * col(num_ratings_col)) / sum(col(num_ratings_col))).alias("Average Rating"), 
                (sum(col(sentiment_col) * col(num_ratings_col)) / sum(col(num_ratings_col))).alias("Sentiment")
            ).select([parent_id_rev, "Reviews", "Total Ratings", "Average Rating", "Sentiment"])
            
            df = df.with_column("Combined", concat(lit("ID: "), col(parent_id_rev), lit(": "), col("Reviews")))
            
            # Convert to pandas
            pd_df = df.select(parent_id_rev, "Reviews", "Total Ratings", "Average Rating", "Sentiment").toPandas()
            
            # Rename columns to standard format if needed
            pd_df = pd_df.rename(columns={
                parent_id_rev: "PARENT_ID_REV",
                "Sentiment": "SENTIMENT"
            })
            
            return pd_df
            
        except Exception as e:
            st.error(f"Error in data processing: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"General error loading data: {str(e)}")
        return pd.DataFrame()

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
    
    if not pd_df.empty:
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
        reviews = pd_df[pd_df["PARENT_ID_REV"] == product_id]["Reviews"].dropna().iloc[0] if not pd_df[pd_df["PARENT_ID_REV"] == product_id]["Reviews"].dropna().empty else ""
        
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
    
    else:
        st.warning("No data available. Please check your Snowflake connection and table access.")

except Exception as e:
    st.error(f"Application error: {str(e)}")
    st.info("To use this app, you need to configure Snowflake credentials. See instructions below.")
    
    st.subheader("Setup Instructions")
    st.markdown("""
    ### Configure Snowflake credentials
    
    #### Option 1: Local Development with secrets.toml
    Create a `.streamlit/secrets.toml` file with:
    ```toml
    [snowflake]
    account = "your_account_id"
    user = "your_username"
    password = "your_password"
    role = "your_role"
    warehouse = "your_warehouse"
    database = "your_database"
    schema = "your_schema"
    ```
    
    #### Option 2: Streamlit Cloud Deployment
    Add the same variables as secrets in the Streamlit Cloud dashboard under "Settings" > "Secrets"
    """)
