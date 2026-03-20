# pip install streamlit pandas sqlalchemy psycopg2-binary plotly
# run the code using "streamlit run dashboard.py"

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

# -----------------------------------------------------------------------------
# 1. Configuration & Database Setup
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="WA Real Estate Investment Analysis", 
    layout="wide", 
    page_icon="📈"
)

@st.cache_resource
def get_db_engine():
    # Pulls securely from .streamlit/secrets.toml
    creds = st.secrets["postgres"]
    db_url = f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
    return create_engine(db_url)

engine = get_db_engine()

# -----------------------------------------------------------------------------
# 2. Independent Data Extraction
# -----------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def get_appreciation_data():
    query = """
        SELECT 
            g.metro, 
            DATE_TRUNC('month', v.metric_date)::DATE AS metric_month, 
            AVG(v.avg_house_value) AS avg_house_value
        FROM fact_house_value v 
        JOIN dim_geography g ON v.geo_key = g.geo_key
        WHERE g.state = 'WA' AND g.metro IS NOT NULL 
        GROUP BY g.metro, metric_month
    """
    df = pd.read_sql(query, engine)
    df['metric_month'] = pd.to_datetime(df['metric_month'])
    return df

@st.cache_data(ttl=3600)
def get_rental_data():
    query = """
        SELECT 
            g.metro, 
            DATE_TRUNC('month', r.metric_date)::DATE AS metric_month, 
            AVG(r.rental_income) AS rental_income
        FROM fact_rental_income r 
        JOIN dim_geography g ON r.geo_key = g.geo_key
        WHERE g.state = 'WA' AND g.metro IS NOT NULL 
        GROUP BY g.metro, metric_month
    """
    df = pd.read_sql(query, engine)
    df['metric_month'] = pd.to_datetime(df['metric_month'])
    return df

@st.cache_data(ttl=3600)
def get_mortgage_data():
    query = """
        SELECT 
            g.metro, 
            DATE_TRUNC('month', m.metric_date)::DATE AS metric_month, 
            AVG(m.monthly_payment) AS monthly_payment
        FROM fact_monthly_payment m 
        JOIN dim_geography g ON m.geo_key = g.geo_key
        WHERE g.state = 'WA' AND g.metro IS NOT NULL 
        GROUP BY g.metro, metric_month
    """
    df = pd.read_sql(query, engine)
    df['metric_month'] = pd.to_datetime(df['metric_month'])
    return df

df_app = get_appreciation_data()
df_rent = get_rental_data()
df_mort = get_mortgage_data()

# -----------------------------------------------------------------------------
# 3. Data Merging & Feature Engineering
# -----------------------------------------------------------------------------
# Outer joins ensure we don't drop a metro just because one metric is missing
df_combined = pd.merge(df_app, df_rent, on=['metro', 'metric_month'], how='outer')
df_combined = pd.merge(df_combined, df_mort, on=['metro', 'metric_month'], how='outer')

# Clean up any weird artifacts and sort
df_combined = df_combined.dropna(subset=['metro', 'metric_month'])
df_combined = df_combined.sort_values(by=['metro', 'metric_month'])

# Investment Metrics
df_combined['gross_yield'] = (df_combined['rental_income'] * 12) / df_combined['avg_house_value']
df_combined['cash_flow'] = df_combined['rental_income'] - df_combined['monthly_payment']
df_combined['price_to_rent_ratio'] = df_combined['avg_house_value'] / (df_combined['rental_income'] * 12)

# -----------------------------------------------------------------------------
# 4. Sidebar UI & Filters
# -----------------------------------------------------------------------------
st.sidebar.title("📍 Market Filters")

all_metros = sorted(df_combined['metro'].unique())
default_metros = [m for m in ['Seattle', 'Spokane', 'Tacoma'] if m in all_metros]
if not default_metros:
    default_metros = all_metros[:3]

selected_metros = st.sidebar.multiselect("Select WA Metros", options=all_metros, default=default_metros)

filtered_df = df_combined[df_combined['metro'].isin(selected_metros)]

# Optional Data Availability Tracker in Sidebar
with st.sidebar.expander("ℹ️ Data Availability Guide"):
    st.markdown("Not all cities have complete datasets. If a chart says 'No Data', it means Zillow has not published that specific metric for that city.")

# -----------------------------------------------------------------------------
# 5. Dashboard Layout (With "No Data" Safeguards)
# -----------------------------------------------------------------------------
st.title("🏠 WA Real Estate Market & Investment Analytics")

if filtered_df.empty:
    st.warning("Please select at least one Metro from the sidebar.")
else:
    # ==========================================
    # SECTION 1: HISTORICAL MARKET TRENDS
    # ==========================================
    st.header("📊 Section 1: Historical Market Trends")
    st.markdown("Raw historical data tracking the foundational metrics of the real estate market.")
    
    tab_h1, tab_h2, tab_h3 = st.tabs(["📈 House Values", "🏠 Rental Income", "🏦 Mortgage Payments"])
    
    with tab_h1:
        valid_app = filtered_df.dropna(subset=['avg_house_value'])
        if not valid_app.empty:
            fig_app = px.line(valid_app, x='metric_month', y='avg_house_value', color='metro', 
                              title="Average House Value Trends", labels={'avg_house_value': 'House Value ($)', 'metric_month': 'Date'})
            st.plotly_chart(fig_app, use_container_width=True)
        else:
            st.info("No House Value data available for the selected metros.")
            
    with tab_h2:
        valid_rent = filtered_df.dropna(subset=['rental_income'])
        if not valid_rent.empty:
            fig_rent = px.line(valid_rent, x='metric_month', y='rental_income', color='metro', 
                               title="Average Monthly Rental Income", labels={'rental_income': 'Monthly Rent ($)', 'metric_month': 'Date'})
            st.plotly_chart(fig_rent, use_container_width=True)
        else:
            st.info("No Rental Income data available for the selected metros.")
            
    with tab_h3:
        valid_mort = filtered_df.dropna(subset=['monthly_payment'])
        if not valid_mort.empty:
            fig_mort = px.line(valid_mort, x='metric_month', y='monthly_payment', color='metro', 
                               title="Estimated Monthly Mortgage Payments", labels={'monthly_payment': 'Mortgage Payment ($)', 'metric_month': 'Date'})
            st.plotly_chart(fig_mort, use_container_width=True)
        else:
            st.info("No Mortgage Payment data available for the selected metros.")

    st.divider()

    # ==========================================
    # SECTION 2: INVESTMENT ANALYSIS
    # ==========================================
    st.header("💼 Section 2: Investment Analysis")
    st.markdown("Derived metrics analyzing Price-to-Rent ratios, Yields, and Net Cash Flow constraints.")
    
    tab_a1, tab_a2 = st.tabs(["⚖️ Rent vs. Value (Yield & Ratios)", "💵 Cash Flow & Affordability"])
    
    with tab_a1:
        col_y1, col_y2 = st.columns(2)
        
        with col_y1:
            # Drop rows missing either value
            valid_yield_df = filtered_df.dropna(subset=['avg_house_value', 'rental_income'])
            
            if not valid_yield_df.empty:
                # 1. Restructure (unpivot) data to plot both metrics on the same timeline
                melted_yield = valid_yield_df.melt(
                    id_vars=['metric_month', 'metro'], 
                    value_vars=['avg_house_value', 'rental_income'], 
                    var_name='Metric', 
                    value_name='Amount ($)'
                )
                
                # Make the labels professional for the legend
                melted_yield['Metric'] = melted_yield['Metric'].map({
                    'avg_house_value': 'House Value', 
                    'rental_income': 'Monthly Rent'
                })
                
                # 2. Line Chart: Rent vs Value Over Time
                fig_rent_val = px.line(
                    melted_yield, 
                    x='metric_month', 
                    y='Amount ($)', 
                    color='metro', 
                    line_dash='Metric',
                    title="House Value vs. Rent Over Time (Log Scale)",
                    # Use a logarithmic scale so the $2,000 rent line isn't squashed by the $500,000 house line
                    log_y=True 
                )
                
                st.plotly_chart(fig_rent_val, use_container_width=True)
            else:
                st.info("No overlapping House Value and Rental data available to calculate trends.")
                
        with col_y2:
            if not valid_yield_df.empty:
                fig_yield = px.line(
                    valid_yield_df, 
                    x='metric_month', 
                    y='gross_yield', 
                    color='metro', 
                    title="Gross Rental Yield Over Time", 
                    labels={'gross_yield': 'Gross Yield', 'metric_month': 'Date'}
                )
                fig_yield.update_layout(yaxis_tickformat='.1%')
                st.plotly_chart(fig_yield, use_container_width=True)
            else:
                st.info("No overlapping House Value and Rental data available to calculate Historical Yield.")

    with tab_a2:
        valid_cash_df = filtered_df.dropna(subset=['rental_income', 'monthly_payment', 'cash_flow'])
        
        if not valid_cash_df.empty:
            # Restructure data to plot both Rent and Mortgage on the same timeline
            melted_cash = valid_cash_df.melt(
                id_vars=['metric_month', 'metro'], 
                value_vars=['rental_income', 'monthly_payment'], 
                var_name='Metric', 
                value_name='Amount ($)'
            )
            # Make the labels look professional in the legend
            melted_cash['Metric'] = melted_cash['Metric'].map({
                'rental_income': 'Rent Income', 
                'monthly_payment': 'Mortgage Payment'
            })
            
            # Line Chart: Rent vs Mortgage Over Time
            fig_rent_mort = px.line(
                melted_cash, 
                x='metric_month', 
                y='Amount ($)', 
                color='metro', 
                line_dash='Metric',
                title="Rent Income vs. Mortgage Payment Over Time"
            )
            st.plotly_chart(fig_rent_mort, use_container_width=True)
            
            # Line Chart: Cash Flow
            fig_cashflow = px.line(
                valid_cash_df, 
                x='metric_month', 
                y='cash_flow', 
                color='metro', 
                title="Estimated Monthly Net Cash Flow (Rent - Mortgage)", 
                labels={'cash_flow': 'Net Cash Flow ($)', 'metric_month': 'Date'}
            )
            fig_cashflow.add_hline(y=0, line_dash="dash", line_color="black", annotation_text="Break Even ($0)")
            st.plotly_chart(fig_cashflow, use_container_width=True)
        else:
            st.info("No overlapping Rental and Mortgage data available to calculate Net Cash Flow trends.")
            
    st.divider()
    
    with st.expander("📂 View Raw Combined Data"):
        st.dataframe(filtered_df.sort_values(by=['metric_month', 'metro'], ascending=[False, True]), use_container_width=True)