import glob

import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path

st.set_page_config(
    page_title="Romanian Real Estate Dashboard",
    page_icon="🏠",
    layout="wide",
)

DATA_DIR = Path(__file__).parent.parent / "data" / "gold"

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------

st.markdown("""
<style>
[data-testid="metric-container"] {
    background-color: #1e1e2e;
    border: 1px solid #313244;
    border-radius: 12px;
    padding: 16px 20px;
}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data
def load_data():
    def read_gold(pattern):
        files = glob.glob(pattern)
        if not files:
            st.error(f"Gold data empty! Rerun pipeline again!")
            st.stop()
        return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

    df_n = read_gold(str(DATA_DIR / "by_neighborhood_csv" / "part-*.csv"))
    df_r = read_gold(str(DATA_DIR / "rooms_distribution_csv" / "part-*.csv"))
    df_s = read_gold(str(DATA_DIR / "market_summary_csv" / "part-*.csv"))
    return df_n, df_r, df_s

df_neighborhood, df_rooms, df_summary = load_data()

SECTORS = sorted(df_neighborhood["Sector"].dropna().unique())

# ---------------------------------------------------------------------------
# Sidebar — filters
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("🏠 Filters")
    st.markdown("---")

    selected_sectors = st.multiselect(
        "Sector",
        options=SECTORS,
        default=SECTORS,
        help="Select one or more city sectors",
    )

    top_n = st.slider(
        "Neighborhoods displayed",
        min_value=5,
        max_value=40,
        value=20,
        step=5,
    )

    metric = st.radio(
        "Primary metric",
        options=["Pret_Mediu_MP_EUR", "Pret_Mediu_EUR"],
        format_func=lambda x: "Avg price €/sqm" if x == "Pret_Mediu_MP_EUR" else "Avg total price €",
    )

    st.markdown("---")
    st.caption("Source: storia.ro · Bucharest")

# ---------------------------------------------------------------------------
# Filter data
# ---------------------------------------------------------------------------

df_filtered = df_neighborhood[df_neighborhood["Sector"].isin(selected_sectors)]

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("Romanian Real Estate Dashboard")
st.caption(f"Showing **{len(df_filtered)}** neighborhoods across **{len(selected_sectors)}** selected sectors")

st.markdown("---")

# ---------------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------------

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total listings",
    f"{int(df_summary['Total_Anunturi'].iloc[0]):,}",
    help="Total number of listings scraped from storia.ro",
)
col2.metric(
    "Avg apartment price",
    f"€ {int(df_summary['Pret_Mediu_EUR'].iloc[0]):,}",
    help="Average asking price across the entire market",
)
col3.metric(
    "Avg price per sqm",
    f"€ {int(df_summary['Pret_Mediu_MP_EUR'].iloc[0]):,} /sqm",
    help="Average price per square meter across the entire market",
)
col4.metric(
    "Highest recorded price",
    f"€ {int(df_summary['Pret_Max_EUR'].iloc[0]):,}",
    help="Most expensive listing in the dataset",
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 1 — Neighborhood bar chart + Room distribution
# ---------------------------------------------------------------------------

col_left, col_right = st.columns([3, 2])

with col_left:
    label = "Avg price €/sqm" if metric == "Pret_Mediu_MP_EUR" else "Avg total price €"
    st.subheader(f"Top {top_n} Neighborhoods — {label}")

    chart_df = (
        df_filtered
        .sort_values(metric, ascending=False)
        .head(top_n)
    )

    fig = px.bar(
        chart_df,
        x=metric,
        y="Neighborhood",
        orientation="h",
        color=metric,
        color_continuous_scale="Blues",
        hover_data={
            "Sector": True,
            "Numar_Anunturi": True,
            "Pret_Mediu_MP_EUR": ":,.0f",
            "Pret_Mediu_EUR": ":,.0f",
            metric: False,
        },
        labels={
            metric: label,
            "Neighborhood": "Neighborhood",
            "Numar_Anunturi": "# Listings",
            "Pret_Mediu_MP_EUR": "€/sqm",
            "Pret_Mediu_EUR": "Avg price €",
        },
    )
    x_dtick = 200_000 if metric == "Pret_Mediu_EUR" else 1_000

    fig.update_layout(
        coloraxis_showscale=False,
        yaxis_title=None,
        yaxis_categoryorder="total ascending",
        xaxis_tickprefix="€ ",
        xaxis_tickformat=",",
        xaxis_dtick=x_dtick,
        height=max(400, top_n * 28),
        margin=dict(l=0, r=20, t=20, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font_color="#fafafa",
    )
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Room Distribution")

    rooms_df = df_rooms.sort_values("Numar_Camere").copy()
    rooms_df["Numar_Camere"] = rooms_df["Numar_Camere"].astype(str) + " cam."

    fig2 = px.bar(
        rooms_df,
        x="Numar_Camere",
        y="Numar_Anunturi",
        color="Numar_Anunturi",
        color_continuous_scale="Blues",
        text="Numar_Anunturi",
        hover_data={
            "Pret_Mediu_EUR": ":,.0f",
            "Numar_Anunturi": True,
            "Numar_Camere": True,
        },
        labels={
            "Numar_Camere": "Rooms",
            "Numar_Anunturi": "# Listings",
            "Pret_Mediu_EUR": "Avg price €",
        },
    )
    fig2.update_traces(textposition="outside")
    fig2.update_layout(
        coloraxis_showscale=False,
        xaxis_title="Number of rooms",
        yaxis_title="Listings",
        height=420,
        margin=dict(l=0, r=20, t=20, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font_color="#fafafa",
    )
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 2 — Top 10 scumpe / ieftine (tabele)
# ---------------------------------------------------------------------------

col_t1, col_t2 = st.columns(2)

COLS_DISPLAY = ["Sector", "Neighborhood", "Numar_Anunturi", "Pret_Mediu_MP_EUR", "Pret_Mediu_EUR"]
COL_RENAME = {
    "Neighborhood":      "Neighborhood",
    "Numar_Anunturi":    "Listings",
    "Pret_Mediu_MP_EUR": "€/sqm",
    "Pret_Mediu_EUR":    "Avg price €",
}

def format_table(df_in, ascending):
    df_out = (
        df_in
        .sort_values("Pret_Mediu_MP_EUR", ascending=ascending)
        .head(10)[COLS_DISPLAY]
        .rename(columns=COL_RENAME)
        .reset_index(drop=True)
    )
    df_out.index += 1
    df_out["€/sqm"]       = df_out["€/sqm"].apply(lambda v: f"€ {v:,.0f}")
    df_out["Avg price €"] = df_out["Avg price €"].apply(lambda v: f"€ {v:,.0f}")
    return df_out

with col_t1:
    st.subheader("🏆 Top 10 Most Expensive Neighborhoods")
    st.dataframe(format_table(df_filtered, ascending=False), use_container_width=True)

with col_t2:
    st.subheader("💰 Top 10 Most Affordable Neighborhoods")
    st.dataframe(format_table(df_filtered, ascending=True), use_container_width=True)
