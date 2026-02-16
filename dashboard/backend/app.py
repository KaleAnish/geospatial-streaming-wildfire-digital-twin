import os
import json
import time
from datetime import datetime, timezone, timedelta

import streamlit as st
import pandas as pd
import pydeck as pdk
from kafka import KafkaConsumer


DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "fire_events")


def parse_event_time(s: str):
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def consumer_factory(bootstrap: str, topic: str, group_id: str, offset_mode: str):
    return KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap],
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset=offset_mode,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=250,
    )


st.set_page_config(layout="wide", page_title="Wildfire Twin (Live)")

# Load frontend styles if present (dashboard/frontend/styles.css)
_frontend_css = os.path.join(os.path.dirname(__file__), "..", "frontend", "styles.css")
if os.path.isfile(_frontend_css):
    with open(_frontend_css, encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.title("Wildfire-Twin — Live Stream Dashboard")
st.caption("Kafka to Streamlit (Phase 0). Spark/Sedona spatial join in Phase 1.")

with st.expander("Connection and display settings", expanded=False):
    r1, r2 = st.columns(2)
    with r1:
        st.subheader("Kafka")
        bootstrap = st.text_input("Bootstrap server", value=DEFAULT_BOOTSTRAP)
        topic = st.text_input("Topic", value=DEFAULT_TOPIC)
    with r2:
        st.subheader("Display")
        refresh_sec = st.slider("Refresh interval (sec)", 0.2, 5.0, 1.0, 0.1)
        max_points = st.slider("Max points to keep", 100, 5000, 800, 100)
        window_min = st.slider("Time window (minutes)", 1, 60, 10)
        temp_threshold = st.slider("Temperature threshold (C)", 0.0, 120.0, 50.0, 1.0)
        offset_mode = st.selectbox("Start offset", ["latest", "earliest"], index=0)
        show_all = st.checkbox("Show all buffered events (ignore time window)", value=False)
    st.subheader("Map")
    map_style_options = {
        "Simple (No Token)": "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        "OpenStreetMap (Voyager)": "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
    }
    style_name = st.selectbox("Map style", list(map_style_options.keys()), index=0)
    map_style = map_style_options[style_name]
    mapbox_api_key = st.text_input("Mapbox token (optional)", type="password")


if "events" not in st.session_state:
    st.session_state.events = []

if "consumer" not in st.session_state:
    st.session_state.consumer = None
    st.session_state.consumer_bootstrap = None
    st.session_state.consumer_topic = None
    st.session_state.consumer_offset_mode = None


need_new_consumer = (
    st.session_state.consumer is None
    or st.session_state.consumer_bootstrap != bootstrap
    or st.session_state.consumer_topic != topic
    or st.session_state.consumer_offset_mode != offset_mode
)

if need_new_consumer:
    try:
        group_id = os.getenv("KAFKA_GROUP_ID", "wildfire_twin_streamlit")
        st.session_state.consumer = consumer_factory(bootstrap, topic, group_id, offset_mode)
        st.session_state.consumer_bootstrap = bootstrap
        st.session_state.consumer_topic = topic
        st.session_state.consumer_offset_mode = offset_mode
        st.toast(f"Connected: {bootstrap} / topic={topic} / offset={offset_mode}", icon=None)
    except Exception as e:
        st.session_state.consumer = None
        st.error(f"Failed to connect to Kafka at {bootstrap} (topic={topic}).\n\n{e}")


def poll_kafka(consumer, max_messages=500, timeout_ms=200):
    if consumer is None:
        return []
    out = []
    try:
        records = consumer.poll(timeout_ms=timeout_ms, max_records=max_messages)
        for _tp, msgs in records.items():
            for m in msgs:
                val = m.value
                if isinstance(val, dict):
                    out.append(val)
    except Exception as e:
        st.warning(f"Kafka poll warning: {e}")
    return out


new = poll_kafka(st.session_state.consumer, max_messages=500)
if new:
    st.session_state.events.extend(new)

with st.expander("Debug (raw stream)", expanded=False):
    st.write("Buffered events:", len(st.session_state.events))
    if len(st.session_state.events) > 0:
        st.json(st.session_state.events[-1])

if len(st.session_state.events) > max_points:
    st.session_state.events = st.session_state.events[-max_points:]

df = pd.DataFrame(st.session_state.events)

if not df.empty:
    for col in ["latitude", "longitude", "temperature"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "event_time" in df.columns:
        df["event_time_parsed"] = df["event_time"].apply(parse_event_time)
    else:
        df["event_time_parsed"] = pd.NaT
    df = df.dropna(subset=["latitude", "longitude"])
    if not show_all:
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(minutes=window_min)
        df = df[df["event_time_parsed"].notna()]
        df = df[df["event_time_parsed"] >= cutoff]
    if "is_fire" not in df.columns:
        df["is_fire"] = df.get("temperature", 0) >= temp_threshold
else:
    df = pd.DataFrame(columns=["event_time", "sensor_id", "latitude", "longitude", "temperature", "is_fire"])
    df["event_time_parsed"] = pd.NaT

c1, c2, c3, c4 = st.columns(4)
c1.metric("Buffered events", len(st.session_state.events))
c2.metric(f"Events (last {window_min}m)", len(df))
c3.metric("Fire events", int(df["is_fire"].sum()) if "is_fire" in df.columns and not df.empty else 0)
c4.metric("Max temp (C)", float(df["temperature"].max()) if "temperature" in df.columns and not df.empty else 0.0)

st.subheader("Live Map")
center_lat, center_lon = 33.98, -117.37
view_state = pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=11, pitch=45)


def color_row(row):
    try:
        t = float(row.get("temperature", 0.0))
        is_fire = bool(row.get("is_fire", False))
    except Exception:
        t, is_fire = 0.0, False
    if is_fire:
        intensity = int(max(0, min(255, (t - temp_threshold) * 6)))
        return [255, max(0, 120 - intensity // 3), 0, 180]
    return [0, 128, 255, 140]


if not df.empty:
    df = df.copy()
    df["color"] = df.apply(color_row, axis=1)
    df["radius"] = df["is_fire"].apply(lambda x: 250 if x else 120)
else:
    df["color"] = []
    df["radius"] = []

layer_points = pdk.Layer(
    "ScatterplotLayer",
    df,
    get_position="[longitude, latitude]",
    get_color="color",
    get_radius="radius",
    pickable=True,
    auto_highlight=True,
)
tooltip = {"text": "sensor: {sensor_id}\nT: {temperature}C\nfire: {is_fire}\ntime: {event_time}"}
deck = pdk.Deck(
    layers=[layer_points],
    initial_view_state=view_state,
    map_style=map_style,
    tooltip=tooltip,
    api_keys={"mapbox": mapbox_api_key} if mapbox_api_key else None,
)
st.pydeck_chart(deck, use_container_width=True)

st.subheader("Latest Events")
df_view = df.sort_values("event_time_parsed", ascending=False).drop(columns=["event_time_parsed"], errors="ignore")
st.dataframe(df_view.head(200), use_container_width=True)

time.sleep(refresh_sec)
st.rerun()
