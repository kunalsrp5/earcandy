import streamlit as st
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import snowflake.connector
import pandas as pd

#page_config
st.set_page_config(
page_title="Earcandy - Streaming Analytics Dashboard",
layout="wide"
)

st.title("Earcandy - Music Streaming Analytics Dashboard")
st.caption("Snowflake • Streamlit • Real-time analytics")

#snowflake connection
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

conn = get_connection()

#kpi cards
kpi_df = pd.read_sql(f"""
SELECT
COUNT(*) AS "total_streams",
COUNT(DISTINCT "uuid") AS "unique_users",
COUNT(*) * 0.008 AS "revenue",
(COUNT(*) * 0.008) / NULLIF(COUNT(DISTINCT "uuid"),0) AS "arpu"
FROM "streams_enriched"
""",conn)

dau_df = pd.read_sql(f"""
SELECT COUNT(DISTINCT "uuid") AS "dau"
FROM "streams_enriched"
WHERE "event_date" = (
SELECT MAX("event_date")
FROM "streams_enriched"
)
""",conn)

total_streams = int(kpi_df["total_streams"][0])
unique_users = int(kpi_df["unique_users"][0])
dau = int(dau_df["dau"][0])
revenue = float(kpi_df["revenue"][0])
arpu = float(kpi_df["arpu"][0])

st.markdown("Key Metrics")

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Total Streams", f"{int(total_streams):,}",help="Total number of streams generated till date")
c2.metric("Unique Users", f"{int(unique_users):,}",help="Total number of users who streamed till date")
c3.metric("DAU", f"{int(dau):,}",help="Daily Active Users (today)")
c4.metric("Revenue", f"${revenue:,.2f}",help="Total revenue generated till date")
c5.metric("ARPU", f"${arpu:,.2f}",help="Average revenue per user")

st.divider()

#genre pie chart + artist word cloud

col1, col2 = st.columns([1, 1])

genre_df = pd.read_sql("""
SELECT "genre", COUNT(*) AS "streams"
FROM "streams_enriched"
WHERE "genre" IS NOT NULL
GROUP BY "genre"
""",conn)

fig_genre = px.pie(
genre_df,
names="genre",
values="streams",
title="Streams Distribution by Genre"
)

col1.plotly_chart(fig_genre, use_container_width=True)

artist_df = pd.read_sql("""
SELECT "artist", COUNT(*) AS "streams"
FROM "streams_enriched"
WHERE "artist" IS NOT NULL
GROUP BY "artist"
ORDER BY "streams" DESC
LIMIT 50;
""",conn)

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
ax.set_title("Top Artists by Streams",fontsize=12,fontweight="bold",pad=20)

col2.pyplot(fig)

st.divider()

#country-wise age-wise listeners bar graph
gen_bar_df = pd.read_sql("""
SELECT
"country" as "Country",
CASE
WHEN "age" >= 60 THEN 'Boomers'
WHEN "age" BETWEEN 25 AND 40 THEN 'Millennials'
WHEN "age" BETWEEN 18 AND 24 THEN 'Gen Z'
ELSE 'Other'
END AS "Generation",
COUNT(*) AS "Streams"
FROM "streams_enriched"
WHERE "country" IS NOT NULL
GROUP BY "Country", "Generation";
""",conn)

fig_gen = px.bar(
gen_bar_df,
x="Country",
y="Streams",
color="Generation",
barmode="group",
title="Music Streaming Habits by Country & Generation"
)

st.plotly_chart(fig_gen, use_container_width=True)

#footer
st.caption("Built with Streamlit • Earcandy Analytics®")
