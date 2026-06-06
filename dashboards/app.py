import glob

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="Romanian Real Estate Dashboard",
    page_icon="🏠",
    layout="wide",
)

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
        return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

    df_n = read_gold("data/gold/by_neighborhood_csv/part-*.csv")
    df_r = read_gold("data/gold/rooms_distribution_csv/part-*.csv")
    df_s = read_gold("data/gold/market_summary_csv/part-*.csv")
    return df_n, df_r, df_s

df_neighborhood, df_rooms, df_summary = load_data()

SECTORS = sorted(df_neighborhood["Sector"].dropna().unique())

# ---------------------------------------------------------------------------
# Sidebar — filtre
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("🏠 Filtre")
    st.markdown("---")

    selected_sectors = st.multiselect(
        "Sector",
        options=SECTORS,
        default=SECTORS,
        help="Selectează unul sau mai multe sectoare",
    )

    top_n = st.slider(
        "Număr cartiere afișate",
        min_value=5,
        max_value=40,
        value=20,
        step=5,
    )

    metric = st.radio(
        "Metrica principală",
        options=["Pret_Mediu_MP_EUR", "Pret_Mediu_EUR"],
        format_func=lambda x: "Preț mediu €/mp" if x == "Pret_Mediu_MP_EUR" else "Preț mediu total €",
    )

    st.markdown("---")
    st.caption("Date: storia.ro · București")

# ---------------------------------------------------------------------------
# Filter data
# ---------------------------------------------------------------------------

df_filtered = df_neighborhood[df_neighborhood["Sector"].isin(selected_sectors)]

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.title("Romanian Real Estate Dashboard")
st.caption(f"Afișând **{len(df_filtered)}** cartiere din **{len(selected_sectors)}** sectoare selectate")

st.markdown("---")

# ---------------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------------

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total anunțuri",
    f"{int(df_summary['Total_Anunturi'].iloc[0]):,}",
    help="Numărul total de anunțuri scrapat din storia.ro",
)
col2.metric(
    "Preț mediu apartament",
    f"€ {int(df_summary['Pret_Mediu_EUR'].iloc[0]):,}",
    help="Media prețurilor de vânzare pe toată piața",
)
col3.metric(
    "Preț mediu pe mp",
    f"€ {int(df_summary['Pret_Mediu_MP_EUR'].iloc[0]):,} /mp",
    help="Prețul mediu per metru pătrat pe toată piața",
)
col4.metric(
    "Preț maxim înregistrat",
    f"€ {int(df_summary['Pret_Max_EUR'].iloc[0]):,}",
    help="Cel mai scump anunț din setul de date",
)

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 1 — Bar chart cartiere + Distribuție camere
# ---------------------------------------------------------------------------

col_left, col_right = st.columns([3, 2])

with col_left:
    label = "Preț mediu €/mp" if metric == "Pret_Mediu_MP_EUR" else "Preț mediu total €"
    st.subheader(f"Top {top_n} cartiere — {label}")

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
            "Neighborhood": "Cartier",
            "Numar_Anunturi": "Nr. anunțuri",
            "Pret_Mediu_MP_EUR": "€/mp",
            "Pret_Mediu_EUR": "Preț mediu €",
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
    st.subheader("Distribuție număr camere")

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
            "Numar_Camere": "Camere",
            "Numar_Anunturi": "Nr. anunțuri",
            "Pret_Mediu_EUR": "Preț mediu €",
        },
    )
    fig2.update_traces(textposition="outside")
    fig2.update_layout(
        coloraxis_showscale=False,
        xaxis_title="Număr camere",
        yaxis_title="Anunțuri",
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
COL_RENAME   = {
    "Neighborhood":    "Cartier",
    "Numar_Anunturi":  "Anunțuri",
    "Pret_Mediu_MP_EUR": "€/mp",
    "Pret_Mediu_EUR":  "Preț mediu €",
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
    df_out["€/mp"]        = df_out["€/mp"].apply(lambda v: f"€ {v:,.0f}")
    df_out["Preț mediu €"] = df_out["Preț mediu €"].apply(lambda v: f"€ {v:,.0f}")
    return df_out

with col_t1:
    st.subheader("🏆 Top 10 cele mai scumpe cartiere")
    st.dataframe(format_table(df_filtered, ascending=False), use_container_width=True)

with col_t2:
    st.subheader("💰 Top 10 cele mai accesibile cartiere")
    st.dataframe(format_table(df_filtered, ascending=True), use_container_width=True)
