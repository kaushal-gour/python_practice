import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import time
import uuid  # ✅ To generate unique keys

# ------------------------------------------------
# PAGE CONFIGURATION
# ------------------------------------------------
st.set_page_config(page_title="Bus Summary Dashboard",
                   page_icon="🚌",
                   layout="wide")

# ------------------------------------------------
# DATABASE CONFIG
# ------------------------------------------------
DB_CONFIG = {
    "dbname": "busdb",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}

REFRESH_INTERVAL = 10  # seconds

# ------------------------------------------------
# FUNCTION: Fetch summary data from PostgreSQL
# ------------------------------------------------
def fetch_summary_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            SELECT 
                batch_id,
                batch_time,
                status,
                count,
                avg_latitude,
                avg_longitude
            FROM warehouse_bus_summary_batch
            ORDER BY batch_time DESC
            LIMIT 50;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

# ------------------------------------------------
# FUNCTION: Render dashboard visuals
# ------------------------------------------------
def render_dashboard(df):
    if df.empty:
        st.warning("No data found in warehouse_bus_summary table yet.")
        return

    latest_batch_time = df["batch_time"].max()
    st.markdown(f"### 🕒 Latest Batch Time: `{latest_batch_time}`")

    total_batches = df["batch_id"].nunique()
    st.metric("Total Recent Batches", total_batches)

    col1, col2 = st.columns(2)

    # 1️⃣ Bus Status Distribution (Pie)
    with col1:
        st.subheader("🧩 Bus Status Distribution")
        latest_df = df[df["batch_time"] == latest_batch_time]

        fig_status = px.pie(
            latest_df,
            names="status",
            values="count",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        # ✅ Unique key every render cycle
        st.plotly_chart(fig_status, use_container_width=True, key=f"pie_{uuid.uuid4()}")

    # 2️⃣ Trends Across Batches (Line Chart)
    with col2:
        st.subheader("📊 Status Trend Over Batches")
        fig_trend = px.line(
            df,
            x="batch_time",
            y="count",
            color="status",
            markers=True,
            title="Bus Status Counts Over Time"
        )
        # ✅ Unique key every render cycle
        st.plotly_chart(fig_trend, use_container_width=True, key=f"trend_{uuid.uuid4()}")

    # 3️⃣ Map Visualization of Average Coordinates
    st.subheader("🗺️ Average Busline Locations (Latest Batch)")
    map_df = latest_df[["avg_latitude", "avg_longitude", "status"]].dropna()
    map_df.rename(columns={"avg_latitude": "lat", "avg_longitude": "lon"}, inplace=True)
    st.map(map_df)

# ------------------------------------------------
# MAIN APP LOGIC
# ------------------------------------------------
st.title("🚌 Real-Time Bus Operations Summary Dashboard")
st.caption("Powered by Kafka → Spark → PostgreSQL → Streamlit")

placeholder = st.empty()

while True:
    df = fetch_summary_data()

    with placeholder.container():
        render_dashboard(df)
        st.info(f"🔄 Dashboard auto-refreshes every {REFRESH_INTERVAL} seconds")

    time.sleep(REFRESH_INTERVAL)