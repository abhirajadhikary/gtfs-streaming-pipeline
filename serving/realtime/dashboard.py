import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="GTFS Live Transit Dashboard", layout="wide")
st.title("GTFS Real-Time Transit Analytics")

API_BASE = os.getenv("API_BASE", "http://localhost:8000/api")


@st.cache_data(ttl=15, show_spinner=False)
def fetch_data(endpoint: str):
    try:
        res = requests.get(f"{API_BASE}/{endpoint}", timeout=5)
        if res.status_code == 200:
            return res.json().get("data", [])
        st.error(f"Serving API returned HTTP {res.status_code} for {endpoint}.")
    except Exception as e:
        st.error(f"Error fetching {endpoint}: {e}")
    return []


def to_dt(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


def to_num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


with st.sidebar:
    st.header("Controls")
    if st.button("Refresh data now"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Data is cached for 15s per endpoint to avoid hammering the API.")

tab1, tab2, tab3 = st.tabs(["Vehicle Positions", "Route Health", "Network Health"])

# ---------------------------------------------------------------------------
# TAB 1 — VEHICLE POSITIONS
# ---------------------------------------------------------------------------
with tab1:
    st.header("Active Vehicle Streams")
    vehicles = fetch_data("vehicles")

    if vehicles:
        df_v = pd.DataFrame(vehicles)
        df_v = to_dt(df_v, ["event_time", "processing_time"])
        df_v = to_num(df_v, ["latitude", "longitude", "speed", "bearing"])

        routes = sorted(df_v["route_id"].dropna().unique().tolist())
        selected_routes = st.multiselect("Filter by route", routes, default=routes, key="veh_route_filter")
        df_v_f = df_v[df_v["route_id"].isin(selected_routes)] if selected_routes else df_v

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Active vehicles", df_v_f["vehicle_id"].nunique())
        c2.metric("Routes running", df_v_f["route_id"].nunique())
        avg_speed = df_v_f["speed"].mean()
        c3.metric("Avg speed", f"{avg_speed:.1f}" if pd.notna(avg_speed) else "—")
        c4.metric("Missing speed reading", int(df_v_f["speed"].isna().sum()))

        map_df = df_v_f.dropna(subset=["latitude", "longitude"])
        if not map_df.empty:
            fig_map = px.scatter_map(
                map_df,
                lat="latitude",
                lon="longitude",
                color="route_id",
                hover_name="vehicle_id",
                hover_data={
                    "trip_id": True,
                    "speed": True,
                    "bearing": True,
                    "latitude": False,
                    "longitude": False,
                },
                zoom=8,
                height=520,
                map_style="open-street-map",
            )
            fig_map.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                legend=dict(orientation="h", y=-0.05),
            )
            st.plotly_chart(fig_map, width="stretch")
        else:
            st.info("No vehicles with valid coordinates to map.")

        cc1, cc2 = st.columns(2)
        with cc1:
            speed_df = df_v_f.dropna(subset=["speed"])
            if not speed_df.empty:
                fig_speed = px.histogram(speed_df, x="speed", nbins=20, title="Speed distribution")
                fig_speed.update_layout(margin=dict(l=10, r=10, t=40, b=10))
                st.plotly_chart(fig_speed, width="stretch")
            else:
                st.info("No speed data available for the current filter.")
        with cc2:
            counts = df_v_f["route_id"].value_counts().reset_index()
            counts.columns = ["route_id", "vehicles"]
            fig_counts = px.bar(counts, x="route_id", y="vehicles", title="Vehicles per route")
            fig_counts.update_layout(margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig_counts, width="stretch")

        with st.expander("Raw vehicle data"):
            st.dataframe(df_v_f, width="stretch")
    else:
        st.info("No active vehicle streams found in Redis.")

# ---------------------------------------------------------------------------
# TAB 2 — ROUTE HEALTH
# ---------------------------------------------------------------------------
with tab2:
    st.header("Route Health & Delays")
    route_health = fetch_data("route-health")

    if route_health:
        df_r = pd.DataFrame(route_health)
        df_r = to_dt(df_r, ["start", "end"])
        df_r = to_num(df_r, ["total_updates_processed"])

        routes = sorted(df_r["route_id"].dropna().unique().tolist())
        selected = st.multiselect("Filter by route", routes, default=routes, key="route_health_filter")
        df_r_f = df_r[df_r["route_id"].isin(selected)] if selected else df_r
        df_r_sorted = df_r_f.sort_values("start")

        c1, c2, c3 = st.columns(3)
        c1.metric("Total updates processed", int(df_r_f["total_updates_processed"].sum()))
        c2.metric("Routes reporting", df_r_f["route_id"].nunique())
        c3.metric("Intervals shown", len(df_r_f))

        if not df_r_sorted.empty:
            fig_line = px.line(
                df_r_sorted,
                x="start",
                y="total_updates_processed",
                color="route_id",
                markers=True,
                title="Updates processed over time, by route",
            )
            fig_line.update_layout(margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig_line, width="stretch")

            latest = df_r_sorted.groupby("route_id").tail(1).sort_values(
                "total_updates_processed", ascending=False
            )
            fig_bar = px.bar(
                latest,
                x="route_id",
                y="total_updates_processed",
                title="Most recent interval — updates by route",
            )
            fig_bar.update_layout(margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig_bar, width="stretch")
        else:
            st.info("No route health rows match the current filter.")

        with st.expander("Raw route health data"):
            st.dataframe(df_r_f, width="stretch")
    else:
        st.info("No route health data found in Redis.")

# ---------------------------------------------------------------------------
# TAB 3 — NETWORK HEALTH
# ---------------------------------------------------------------------------
with tab3:
    st.header("Overall Network Health")
    network_health = fetch_data("network-health")

    if network_health:
        df_n = pd.DataFrame(network_health)
        df_n = to_dt(df_n, ["processing_time"])

        causes = sorted(df_n["cause"].dropna().unique().tolist())
        effects = sorted(df_n["effect"].dropna().unique().tolist())
        colf1, colf2 = st.columns(2)
        sel_causes = colf1.multiselect("Filter by cause", causes, default=causes)
        sel_effects = colf2.multiselect("Filter by effect", effects, default=effects)
        df_n_f = df_n[df_n["cause"].isin(sel_causes) & df_n["effect"].isin(sel_effects)]

        c1, c2, c3 = st.columns(3)
        c1.metric("Total alerts", len(df_n_f))
        c2.metric("Distinct causes", df_n_f["cause"].nunique())
        c3.metric("Distinct effects", df_n_f["effect"].nunique())

        cc1, cc2 = st.columns(2)
        with cc1:
            cause_counts = df_n_f["cause"].value_counts().reset_index()
            cause_counts.columns = ["cause", "alerts"]
            fig_cause = px.bar(cause_counts, x="cause", y="alerts", title="Alerts by cause")
            fig_cause.update_layout(margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig_cause, width="stretch")
        with cc2:
            effect_counts = df_n_f["effect"].value_counts().reset_index()
            effect_counts.columns = ["effect", "alerts"]
            fig_effect = px.bar(effect_counts, x="effect", y="alerts", title="Alerts by effect")
            fig_effect.update_layout(margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig_effect, width="stretch")

        st.subheader("Alert details")
        if df_n_f.empty:
            st.info("No alerts match the current filter.")
        else:
            for _, row in df_n_f.iterrows():
                label = f"[{row.get('alert_id', '')}] {row.get('cause', '')} / {row.get('effect', '')}"
                with st.expander(label):
                    st.write(row.get("header_text", ""))
                    st.caption(f"Processed at {row.get('processing_time', '')}")

        with st.expander("Raw network health data"):
            st.dataframe(df_n_f, width="stretch")
    else:
        st.info("No network health metrics found in Redis.")