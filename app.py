import streamlit as st
#from snowflake.snowpark.context import get_active_session
import snowflake.connector
import pandas as pd

# ----------------------------------------------------
# Session
# ----------------------------------------------------
#session = get_active_session()

st.set_page_config(
page_title="Music Streaming Dashboard",
layout="wide"
)

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

def top_album_card(album, artist, year, artwork_url):
    st.markdown(
    f"""
    <div style="
    max-width:360px;
    background:#ffffff;
    border-radius:18px;
    padding:16px;
    text-align:center;
    box-shadow:0 8px 20px rgba(0,0,0,0.1);
    ">
    <div style="
    font-weight:bold;
    height:35px;
    text-align:left;
    ">Top Album</div>
    <img style="
    border-radius:14px;"
    src={artwork_url}></img>
    <div style="
    font-weight:700;
    margin-top:12px;
    font-size:16px;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;"
    title="{album}"
    >{album}</div>
    <div style="
    font-size:14px;
    color:#666;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;"
    title="{artist}"
    >{artist}</div>
    
    </div>
    """,
    unsafe_allow_html=True
    )
    
def song_card(title, artist, artwork_url):
    st.markdown(
    f"""
    <div style="
    width:200px;
    height:280px;
    background:#ffffff;
    border-radius:16px;
    padding:12px;
    box-shadow:0 6px 16px rgba(0,0,0,0.12);
    display:flex;
    flex-direction:column;
    align-items:center;
    ">
    <img src="{artwork_url}"
    style="width:100%; height:160px; object-fit:cover; border-radius:12px;" />
    
    <div style="
    margin-top:10px;
    font-weight:600;
    font-size:14px;
    width:100%;
    text-align:center;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;"
    title="{title}">
    {title}
    </div>
    
    <div style="
    font-size:12px;
    color:#666;
    width:100%;
    text-align:center;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;"
    title="{artist}">
    {artist}
    </div>
    </div>
    """,
    unsafe_allow_html=True
    )
# --------------------------------------------------
# UI THEME (Dark, modern)
# --------------------------------------------------
st.markdown("""
<style>
.card{
position:relative;
padding:12px;
border-radius:14px;
text-align:center;
}
.song-title{
font-weight:600;
font-size: 0.95rem;
overflow:hidden;
height:1.2em;
line-height:1.2em;
}
.song-artist{
font-size: 0.85rem;
color:#8b949e;
line-height:1.2em;
height:1.2em;
overflow:hidden;
}
.main { background-color: #0E1117; color: #FAFAFA; }

[data-testid="metric-container"] {
background-color: #161B22;
border: 1px solid #30363D;
padding: 18px;
border-radius: 14px;
}

h1, h2, h3 { color: #58A6FF; }

div[role="radiogroup"] > label{
background-color:#FFFFFF;
padding: 8px 16px;
border-radius:10px;
margin-right:8px;
}

.stDataFrame { border-radius: 12px; }
</style>
""", unsafe_allow_html=True)

st.title("Earcandy - Music Streaming Analytics Dashboard")
st.caption("Snowflake • Streamlit • Real-time analytics")

# --------------------------------------------------
# SIDEBAR FILTERS
# --------------------------------------------------
st.sidebar.header("Filters")

date_range = pd.read_sql("""
SELECT MIN("event_date") AS MIN_DATE, MAX("event_date") AS MAX_DATE
FROM "streams_enriched"
""",conn)

#start_date, end_date = st.sidebar.date_input(
#"Event Date Range",
#[date_range.MIN_DATE[0], date_range.MAX_DATE[0]]
#)

date_input = st.sidebar.date_input(
    "Date Range",
    [date_range["MIN_DATE"][0], date_range["MAX_DATE"][0]]
)
#handle single-date break
if not isinstance(date_input,(list,tuple)) or len(date_input)!=2:
    st.sidebar.warning("Please select both a start and end date")
    st.stop()

start_date, end_date = date_input

country_df = pd.read_sql("""
SELECT DISTINCT "country"
FROM "streams_enriched"
WHERE "country" IS NOT NULL
""",conn)

country_filter = st.sidebar.multiselect("Country",country_df["country"].tolist())
country_clause = ""
if country_filter:
    quoted_countries = ",".join([f"'{c}'" for c in country_filter])
    country_clause = f'AND country IN ({quoted_countries})'


# ----------------------------------------------------
# KPI QUERIES
# ----------------------------------------------------
kpi_df = pd.read_sql(f"""
SELECT
COUNT(*) AS total_streams,
COUNT(DISTINCT "uuid") AS unique_users,
COUNT(*) * 0.008 AS revenue,
(COUNT(*) * 0.008) / NULLIF(COUNT(DISTINCT "uuid)",0) AS arpu
FROM "streams_enriched"
WHERE "event_date" BETWEEN '{start_date}' AND '{end_date}'
{country_clause}
""",conn)

# DAU
dau_df = pd.read_sql(f"""
SELECT COUNT(DISTINCT "uuid") AS dau
FROM "streams_enriched"
WHERE "event_date" = (
SELECT MAX("event_date")
FROM "streams_enriched"
)
{country_clause}
""",conn)

total_streams = int(kpi_df["total_streams"][0])
unique_users = int(kpi_df["unique_users"][0])
dau = int(dau_df["dau"][0])
revenue = float(kpi_df["revenue"][0])
arpu = float(kpi_df["arpu"][0])

# --------------------------------------------------
# KPI DISPLAY
# --------------------------------------------------
st.markdown("## 🔑 Key Metrics")

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("🎧 Total Streams", f"{int(total_streams):,}")
c2.metric("👥 Unique Users", f"{int(unique_users):,}")
c3.metric("⚡ DAU", f"{int(dau):,}")
c4.metric("💰 Revenue", f"${revenue:,.2f}")
c5.metric("📈 ARPU", f"${arpu:,.2f}")


#------kpi end--------
st.divider()

# --------------------------------------------------
# SECTION SWITCHER
# --------------------------------------------------

selected_tab = st.radio(
    "",
    ["Overview","Content","Trends","Retention","Demographics"],
    horizontal=True
)
# --------------------------------------------------
# TAB 1 – OVERVIEW
# --------------------------------------------------

if selected_tab == "Overview":
    left_col, right_col = st.columns([1,2.2])
    with left_col:
        album_df = pd.read_sql(f"""
        SELECT
        "album",
        "artist",
        "release_year",
        "artwork_url",
        COUNT(*) AS streams
        FROM "streams_enriched"
        WHERE "event_date" BETWEEN '{start_date}' AND '{end_date}'
        {country_clause}
        GROUP BY "album", "artist", "release_year", "artwork_url"
        ORDER BY "streams" DESC
        LIMIT 1
        """,conn)
    
        row = album_df.iloc[0]
        top_album_card(
            album=row['album'],
            artist=row['artist'],
            year=row['release_year'],
            artwork_url=row['artwork_url']
        )

    with right_col:
        st.markdown("""
        <div style="
        font-weight:bold;"
        >New Releases</div>
        """,unsafe_allow_html=True)

        new_releases_df = pd.read_sql(f"""
        select "title","artist","artwork_url"
        from "streams_enriched"
        where "release_year" in (
        select max("release_year") from (
        select "release_year" from "streams_enriched"
        where "release_year" is not null
        and "event_date" BETWEEN '{start_date}' AND '{end_date}'
        {country_clause}
        group by "release_year"
        )
        )
        group by "title","artist","artwork_url"
        limit 4;
        """,conn)
        

        cols = st.columns(4)
        for i, row in new_releases_df.iterrows():
            with cols[i]:
                song_card(row["title"], row["artist"], row["artwork_url"])
    #st.subheader("User Activity Over Time")

    #trend_sql = f"""
    #SELECT "event_date",
    #COUNT(*) AS streams,
    #COUNT(DISTINCT "uuid") AS dau
    #FROM "EARCANDY_DB"."PUBLIC"."streams_enriched"
    #WHERE "event_date" BETWEEN '{start_date}' AND '{end_date}'
    #{country_clause}
    #GROUP BY "event_date"
    #ORDER BY "event_date"
    #"""
    
    #trend_df = session.sql(trend_sql).to_pandas().set_index('event_date')
    
    #col1, col2 = st.columns(2)
    #col1.line_chart(trend_df["STREAMS"])
    #col2.line_chart(trend_df["DAU"])
    
    #st.info("📌 DAU reflects daily engagement, streams reflect consumption volume.")
    
    #st.divider()

# --------------------------------------------------
# TAB 2 – CONTENT
# --------------------------------------------------
elif selected_tab == "Content":
    st.subheader("Top Performing Content")

    top_songs_df = pd.read_sql(f"""
    SELECT "title",
    COUNT(*) AS streams
    FROM "streams_enriched"
    WHERE "event_date" BETWEEN '{start_date}' AND '{end_date}'
    {country_clause}
    GROUP BY "title"
    ORDER BY streams DESC
    LIMIT 10
    """,conn)
    
    top_artists_df = pd.read_sql(f"""
    SELECT "artist",
    COUNT(*) AS streams
    FROM "streams_enriched"
    WHERE "event_date" BETWEEN '{start_date}' AND '{end_date}'
    {country_clause}
    GROUP BY "artist"
    ORDER BY streams DESC
    LIMIT 10
    """,conn)
    
    top_songs = top_songs_df.set_index("title")
    top_artists = top_artists_df.set_index("artist")
    
    col1, col2 = st.columns(2)
    col1.bar_chart(top_songs)
    col2.bar_chart(top_artists)
    
    st.divider()

# --------------------------------------------------
# TAB 3 – TRENDING
# --------------------------------------------------
elif selected_tab == "Trends":
    st.subheader("🔥 Trending Songs (Last 7 Days)")

    trending_df = pd.read_sql(f"""
    SELECT "title",
    "artist",
    "artwork_url",
    "preview_url",
    COUNT(*) AS "streams_last_7_days"
    FROM "streams_enriched"
    WHERE "event_date" >= CURRENT_DATE - 7
    {country_clause}
    and "artwork_url" is not null
    and "preview_url" is not null
    GROUP BY "title","artist","artwork_url","preview_url"
    ORDER BY "streams_last_7_days" DESC
    LIMIT 10
    """,conn)
    
    # Display cards (5 per row)
    cols = st.columns(5)
    
    for idx, row in trending_df.iterrows():
        song_key = f"{row['title']}|{row['artist']}"
        with cols[idx % 5]:
            #card
            st.markdown('<div class="card>', unsafe_allow_html=True)
            #artwork
            st.image(row['artwork_url'], use_container_width=True)
            #audio
            st.audio(row['preview_url'])
            #text
            st.markdown(
            f"""
            <div class="song-title">{row['title']}</div>
            <div class="song-artist">{row['artist']}</div>
            """,
            unsafe_allow_html=True
            )
            #streams
            st.caption(f"🔥 {int(row['streams_last_7_days']):,} streams")
            
            st.markdown("</div>", unsafe_allow_html=True)
    #info        
    st.info("Trending songs are ranked by streams in the last 7 days.")
    
    st.divider()

# ----------------------------------------------------
# RETENTION METRICS
# ----------------------------------------------------
elif selected_tab == "Retention":
    st.subheader("🔁 Retention Metrics")

    retention_df = pd.read_sql(f"""
    SELECT
    COUNT(DISTINCT CASE WHEN "event_date" = "registered_on" + 1 THEN "uuid" END)
    / NULLIF(COUNT(DISTINCT "uuid"),0) * 100 AS day1_retention,
    
    COUNT(DISTINCT CASE WHEN "event_date" = "registered_on" + 7 THEN "uuid" END)
    / NULLIF(COUNT(DISTINCT "uuid"),0) * 100 AS day7_retention,
    
    COUNT(DISTINCT CASE WHEN "event_date" = "registered_on" + 30 THEN "uuid" END)
    / NULLIF(COUNT(DISTINCT "uuid"),0) * 100 AS day30_retention
    FROM "streams_enriched"
    WHERE "registered_on" BETWEEN '{start_date}' AND '{end_date}'
    {country_clause}
    """,conn)
    
    ret = retention_df.iloc[0]
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Day 1 Retention", f"{ret.DAY1_RETENTION:.2f}%")
    c2.metric("Day 7 Retention", f"{ret.DAY7_RETENTION:.2f}%")
    c3.metric("Day 30 Retention", f"{ret.DAY30_RETENTION:.2f}%")
    
    st.divider()

# --------------------------------------------------
# TAB 5 – DEMOGRAPHICS
# --------------------------------------------------
elif selected_tab == "Demographics":
    st.subheader("Audience Breakdown")

    gender_df = pd.read_sql(ff"""
    SELECT "gender",
    COUNT(DISTINCT "uuid") AS users
    FROM "streams_enriched"
    WHERE "event_date" BETWEEN '{start_date}' AND '{end_date}'
    {country_clause}
    GROUP BY "gender"
    """,conn)
    
    country_df = pd.read_sql(ff"""
    SELECT "country",
    COUNT(*) AS streams
    FROM "streams_enriched"
    WHERE "event_date" BETWEEN '{start_date}' AND '{end_date}'
    {country_clause}
    GROUP BY "country"
    ORDER BY streams DESC
    LIMIT 10
    """,conn)
    
    gender_dff = gender_df.set_index("gender")
    country_dff = country_df.set_index("country")
    
    col1, col2 = st.columns(2)
    col1.bar_chart(gender_dff)
    col2.bar_chart(country_dff)

st.divider()
st.caption("Built with Streamlit • Earcandy Analytics®")
