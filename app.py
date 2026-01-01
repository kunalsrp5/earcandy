import streamlit as st
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

# ----------------------------------
# Page config
# ----------------------------------
st.set_page_config(
    page_title="Earcandy - Streaming Analytics Dashboard",
    layout="wide"
)

st.title("Earcandy - Music Streaming Analytics Dashboard")
st.caption("Neon Postgres • Streamlit • Real-time analytics")

# ----------------------------------
# Neon Postgres connection
# ----------------------------------
@st.cache_resource
def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{st.secrets['postgres']['user']}:"
        f"{st.secrets['postgres']['password']}@"
        f"{st.secrets['postgres']['host']}:"
        f"{st.secrets['postgres']['port']}/"
        f"{st.secrets['postgres']['database']}?sslmode=require"
    )

engine = get_engine()

# ----------------------------------
# KPI cards
# ----------------------------------
kpi_df = pd.read_sql("""
SELECT
    COUNT(*)                       AS total_streams,
    COUNT(DISTINCT uuid)           AS unique_users,
    COUNT(*) * 0.008               AS revenue,
    (COUNT(*) * 0.008)
        / NULLIF(COUNT(DISTINCT uuid), 0) AS arpu
FROM streams_enriched;
""", engine)

dau_df = pd.read_sql("""
SELECT COUNT(DISTINCT uuid) AS dau
FROM streams_enriched
WHERE event_date = (
    SELECT MAX(event_date) FROM streams_enriched
);
""", engine)

total_streams = int(kpi_df.loc[0, "total_streams"])
unique_users  = int(kpi_df.loc[0, "unique_users"])
revenue       = float(kpi_df.loc[0, "revenue"])
arpu          = float(kpi_df.loc[0, "arpu"])
dau           = int(dau_df.loc[0, "dau"])

st.markdown("### Key Metrics")

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Total Streams", f"{total_streams:,}")
c2.metric("Unique Users", f"{unique_users:,}")
c3.metric("DAU", f"{dau:,}")
c4.metric("Revenue", f"${revenue:,.2f}")
c5.metric("ARPU", f"${arpu:,.2f}")

st.divider()

# ----------------------------------
# Genre distribution + Artist word cloud
# ----------------------------------
col1, col2 = st.columns(2)

genre_df = pd.read_sql("""
SELECT genre, COUNT(*) AS streams
FROM streams_enriched
WHERE genre IS NOT NULL
GROUP BY genre;
""", engine)

fig_genre = px.pie(
    genre_df,
    names="genre",
    values="streams",
    title="Streams Distribution by Genre"
)
col1.plotly_chart(fig_genre, use_container_width=True)

artist_df = pd.read_sql("""
SELECT artist, COUNT(*) AS streams
FROM streams_enriched
WHERE artist IS NOT NULL
GROUP BY artist
ORDER BY streams DESC
LIMIT 50;
""", engine)

wordcloud = WordCloud(
    width=800,
    height=450,
    background_color="white"
).generate_from_frequencies(
    dict(zip(artist_df["artist"], artist_df["streams"]))
)

fig, ax = plt.subplots(figsize=(8, 4))
ax.imshow(wordcloud, interpolation="bilinear")
ax.axis("off")
ax.set_title("Top Artists by Streams", fontweight="bold")

col2.pyplot(fig)

st.divider()

# ----------------------------------
# Country vs Generation
# ----------------------------------
gen_bar_df = pd.read_sql("""
SELECT
    country AS "Country",
    CASE
        WHEN age >= 60 THEN 'Boomers'
        WHEN age BETWEEN 25 AND 40 THEN 'Millennials'
        WHEN age BETWEEN 18 AND 24 THEN 'Gen Z'
        ELSE 'Other'
    END AS "Generation",
    COUNT(*) AS "Streams"
FROM streams_enriched
WHERE country IS NOT NULL
GROUP BY country, "Generation";
""", engine)

fig_gen = px.bar(
    gen_bar_df,
    x="Country",
    y="Streams",
    color="Generation",
    barmode="group",
    title="Music Streaming Habits by Country & Generation"
)

st.plotly_chart(fig_gen, use_container_width=True)

st.divider()

# ----------------------------------
# Streams over time
# ----------------------------------
streams_time_df = pd.read_sql("""
SELECT
    event_date,
    COUNT(*) AS total_streams
FROM streams_enriched
WHERE event_date IS NOT NULL
GROUP BY event_date
ORDER BY event_date;
""", engine)

fig = px.line(
    streams_time_df,
    x="event_date",
    y="total_streams",
    markers=True,
    title="Streaming Activity Over Time",
    labels={
        "event_date": "Date",
        "total_streams": "Total Streams"
    }
)

fig.update_layout(hovermode="x unified", height=350)

st.plotly_chart(fig, use_container_width=True)

# ----------------------------------
# Footer
# ----------------------------------
st.caption("Built with Streamlit • Earcandy Analytics®")
