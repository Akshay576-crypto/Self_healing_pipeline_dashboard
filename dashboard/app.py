import sys
import os

# Fix import path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
from pipeline.executor import PipelineExecutor
from database.fetch import FetchData

# -------------------------------
# PAGE CONFIG
# -------------------------------
st.set_page_config(
    page_title="Self-Healing Pipeline",
    page_icon="🚀",
    layout="wide"
)

# -------------------------------
# CUSTOM UI STYLE
# -------------------------------
st.markdown("""
<style>
.big-title {
    font-size: 40px;
    font-weight: bold;
    color: #4CAF50;
}
</style>
""", unsafe_allow_html=True)

# -------------------------------
# TITLE
# -------------------------------
st.markdown('<p class="big-title">🚀 Self-Healing Data Pipeline Dashboard</p>', unsafe_allow_html=True)
st.write("---")

# -------------------------------
# SIDEBAR
# -------------------------------
st.sidebar.header("⚙ Controls")

if st.sidebar.button("▶ Run Pipeline"):
    with st.spinner("Running pipeline..."):
        PipelineExecutor.run()
    st.success(" Pipeline executed successfully!")

# -------------------------------
# METRICS SECTION (REAL DATA)
# -------------------------------
st.subheader("📊 Pipeline Overview")

try:
    pipeline_runs = FetchData.fetch_pipeline_runs()

    if pipeline_runs:
        latest = pipeline_runs[0]

        total_records = latest.get("total_records", 0)
        error_records = latest.get("error_count", 0)
        success_records = total_records - error_records
        quality_score = latest.get("quality_score", 0)

    else:
        total_records = error_records = success_records = quality_score = 0

except Exception as e:
    total_records = error_records = success_records = quality_score = 0
    st.error(f"Error fetching metrics: {e}")

col1, col2, col3, col4 = st.columns(4)

col1.metric("📦 Total Records", total_records)
col2.metric("❌ Errors", error_records)
col3.metric("✅ Cleaned", success_records)
col4.metric("📊 Quality Score", f"{quality_score}%")

st.write("---")

# 📈 Quality Score Trend
st.subheader("📈 Quality Score Trend")

try:
    runs = FetchData.fetch_pipeline_runs()

    if runs:
        df = pd.DataFrame(runs)
        df = df[::-1]

        st.line_chart(df["quality_score"])
    else:
        st.info("No data for chart")

except Exception as e:
    st.error(f"Chart error: {e}")

# -------------------------------
# ERROR VS CLEANED
# -------------------------------
st.subheader("📊 Error vs Cleaned")

try:
    if runs:
        df_chart = pd.DataFrame(runs)

        df_chart["cleaned"] = df_chart["total_records"] - df_chart["error_count"]

        st.bar_chart(df_chart[["error_count", "cleaned"]])
    else:
        st.info("No data for chart")

except Exception as e:
    st.error(f"Chart error: {e}")

# -------------------------------
# ERROR DATA
# -------------------------------
st.subheader("🚨 Error Data")

try:
    error_data = FetchData.fetch_error_records()

    if error_data:
        df_error = pd.DataFrame(error_data)
        st.dataframe(df_error, use_container_width=True)
    else:
        st.success(" No error records found!")

except Exception as e:
    st.error(f"Error fetching error data: {e}")

st.write("---")

# -------------------------------
# CLEANED DATA
# -------------------------------
st.subheader("✅ Cleaned Data")

try:
    cleaned_data = FetchData.fetch_cleaned_records()

    if cleaned_data:
        df_clean = pd.DataFrame(cleaned_data)
        st.dataframe(df_clean, use_container_width=True)
    else:
        st.info("No cleaned data found")

except Exception as e:
    st.error(f"Error fetching cleaned data: {e}")

st.write("---")

# -------------------------------
# PIPELINE HISTORY
# -------------------------------
st.subheader("📈 Pipeline Run History")

try:
    runs = FetchData.fetch_pipeline_runs()

    if runs:
        df_runs = pd.DataFrame(runs)
        st.dataframe(df_runs, use_container_width=True)
    else:
        st.info("No pipeline run history")

except Exception as e:
    st.error(f"Error fetching pipeline runs: {e}")

st.write("---")

# -------------------------------
# INFO SECTION
# -------------------------------
st.subheader("ℹ About System")

st.info("""
This system automatically:
- Fetches data from API  
- Validates and cleans data  
- Fixes errors using rule engine  
- Reprocesses failed records  
- Calculates data quality score  
""")
