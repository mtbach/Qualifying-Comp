import pandas as pd
import numpy as np
import snowflake.connector
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State
from dash.exceptions import PreventUpdate
import os
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    "user": os.getenv("USER"),
    "password": os.getenv("PASSWORD"),
    "account": os.getenv("ACCOUNT"),
    "warehouse": os.getenv("WAREHOUSE"),
    "database": os.getenv("DATABASE"),
    "schema": os.getenv("SCHEMA"),
    "role": os.getenv("ROLE")
}
conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
curr = conn.cursor()

def load_session_data(SESSION_ID, DRIVER_NUMBER):
    query1 = (f"""SELECT time_start from fct_openf1_api__fastest_lap WHERE session_id = {SESSION_ID} AND driver_number = {DRIVER_NUMBER};""")
    res = curr.execute(query1).fetchone()
    time_start = res[0]
    query2 = (f"""SELECT timeadd(second, lap_duration, time_start) from fct_openf1_api__fastest_lap where session_id = {SESSION_ID} and driver_number = {DRIVER_NUMBER};""")
    res = curr.execute(query2).fetchone()
    time_end = res[0]
    
    query = f"""
    
        SELECT
            driver_number,
            time,
            x,
            y,
            speed,
            rpm,
            n_gear, 
            throttle,
            brake
        FROM fct_openf1_api__telemetry_{str(DRIVER_NUMBER)}
        WHERE session_id = {SESSION_ID}
          AND time >= to_timestamp_ntz(%s) AND time <= to_timestamp_ntz(%s)
        ORDER BY time
    """
    
    df = curr.execute(query, (time_start, time_end)).fetch_pandas_all()
    return df

def preprocess_driver(df):
    df = df.sort_values("TIME").reset_index(drop=True)

    dx = df["X"].diff()
    dy = df["Y"].diff()

    df["SEG_DIST"] = np.sqrt(dx**2 + dy**2).fillna(0)
    df["CUM_DIST"] = df["SEG_DIST"].cumsum()
    df["PROGRESS"] = df["CUM_DIST"] / df["CUM_DIST"].iloc[-1]

    return df


def add_lap_distance(df):
    dx = df["X"].diff()
    dy = df["Y"].diff()

    df["SEG_DIST"] = np.sqrt(dx**2 + dy**2).fillna(0)
    df["CUM_DIST"] = df["SEG_DIST"].cumsum()

    total_dist = df["CUM_DIST"].iloc[-1]
    df["PROGRESS"] = df["CUM_DIST"] / total_dist

    return df

def row_at_progress(df, progress):
    idx = (df["PROGRESS"] - progress).abs().idxmin()
    return df.loc[idx]

def progress_from_xy(x, y):
    dx = TRACK_X - x
    dy = TRACK_Y - y
    dist = dx**2 + dy**2
    idx = dist.argmin()
    return TRACK_PROGRESS[idx]


SESSION_ID = 9935
DRIVERS = {4, 81}

df_81 = load_session_data(SESSION_ID, 81)
df_4 = load_session_data(SESSION_ID, 4)

df_81 = preprocess_driver(df_81)
df_4= preprocess_driver(df_4)

TRACK_X = df_81["X"].values
TRACK_Y = df_81["Y"].values
TRACK_PROGRESS = df_81["PROGRESS"].values


DRIVERS = {
    "81": df_81,
    "4": df_4
}

TIME_SERIES = {
    d: df["TIME"]
    for d, df in DRIVERS.items()
}

def base_circuit_figure():
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=TRACK_X,
            y=TRACK_Y,
            mode="lines",
            name="Circuit",
            line=dict(color="gray"),
            hoverinfo=None
        )
    )
    fig.add_trace(
        go.Scatter(
            x=TRACK_X,
            y=TRACK_Y,
            mode="markers",
            marker=dict(size=8, opacity=0),
            hoverinfo="none",
            name="hover_points"
        )
    )

    fig.update_layout(
        dragmode="pan",  
        hovermode="closest",
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        height=600,
        uirevision="track",
        margin=dict(l=20, r=20, t=20, b=20)
    )

    return fig

def time_series_figure(df, metric, color):
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["TIME"],
            y=df[metric],
            mode="lines",
            line=dict(color=color),
            name=metric
        )
    )


    fig.update_layout(
        height=180,
        margin=dict(l=30, r=10, t=10, b=20),
        xaxis_title=None,
        yaxis_title=metric,
        template="plotly_dark"
    )

    return fig

def add_cursor(fig, cursor_time):
    fig = go.Figure(fig)  
    fig.update_layout(
        shapes=[
            dict(
                type="line",
                x0=cursor_time,
                x1=cursor_time,
                y0=0,
                y1=1,
                xref="x",
                yref="paper",
                line=dict(color="white", width=2, dash="dash")
            )
        ]
    )

    return fig

BASE_METRIC_FIGS = {
    "81": {
        "SPEED":   time_series_figure(df_81, "SPEED", "#e10600"),
        "THROTTLE":time_series_figure(df_81, "THROTTLE", "#e10600"),
        "N_GEAR":  time_series_figure(df_81, "N_GEAR", "#e10600"),
        "BRAKE":   time_series_figure(df_81, "BRAKE", "#e10600"),
    },
    "4": {
        "SPEED":   time_series_figure(df_4, "SPEED", "#00d2be"),
        "THROTTLE":time_series_figure(df_4, "THROTTLE", "#00d2be"),
        "N_GEAR":  time_series_figure(df_4, "N_GEAR", "#00d2be"),
        "BRAKE":   time_series_figure(df_4, "BRAKE", "#00d2be"),
    }
}

def metrics_panel(driver, df, row):
    figs = BASE_METRIC_FIGS[driver]

    return html.Div([
        html.H3(f"Driver {driver}", style={"marginBottom": "12px"}),

        dcc.Graph(
            figure=add_cursor(figs["SPEED"], row["TIME"]),
            config={"displayModeBar": False},
        ),

        dcc.Graph(
            figure=add_cursor(figs["THROTTLE"], row["TIME"]),
            config={"displayModeBar": False}
        ),

        dcc.Graph(
            figure=add_cursor(figs["N_GEAR"], row["TIME"]),
            config={"displayModeBar": False}
        ),

        dcc.Graph(
            figure=add_cursor(figs["BRAKE"], row["TIME"]),
            config={"displayModeBar": False}
        )
    ])



app = Dash(__name__)

app.layout = html.Div(
    children=[
        html.Div(
            id="metrics-left",
            style={
                "width": "20%",
                "padding": "16px",
                "backgroundColor": "#111",
                "color": "white",
                "borderRight": "1px solid #333"
            }
        ),

        html.Div(
            children=[
                dcc.Graph(
                    id="circuit",
                    figure=base_circuit_figure(),
                    clear_on_unhover=False,
                    style={"height": "100%"}
                )
            ],
            style={
                "width": "60%",
                "display": "flex",
                "justifyContent": "center",
                "alignItems": "center"
            }
        ),

        html.Div(
            id="metrics-right",
            style={
                "width": "20%",
                "padding": "16px",
                "backgroundColor": "#111",
                "color": "white",
                "borderLeft": "1px solid #333"
            }
        ),
    ],
    style={
        "display": "flex",
        "height": "100vh",
        "backgroundColor": "#0b0b0b",
        "fontFamily": "Arial, sans-serif"
    }
)



@app.callback(
    Output("circuit", "figure"),
    Output("metrics-left", "children"),
    Output("metrics-right", "children"),
    Input("circuit", "clickData"),
    State("circuit", "figure")
)
def scrub_track(clickData, fig):
    if clickData is None:
        raise PreventUpdate

    point_idx = clickData["points"][0]["pointIndex"]
    progress = TRACK_PROGRESS[point_idx]

    row_81 = row_at_progress(DRIVERS["81"], progress)
    row_4 = row_at_progress(DRIVERS["4"], progress)

    fig["data"] = fig["data"][:2]

    
    fig["data"].append({
        "type": "scatter",
        "x": [row_81["X"]],
        "y": [row_81["Y"]],
        "mode": "markers",
        "marker": {"size": 14},
        "name": "Driver 81",
        "hoverinfo": "skip"
    })

    fig["data"].append({
        "type": "scatter",
        "x": [row_4["X"]],
        "y": [row_4["Y"]],
        "mode": "markers",
        "marker": {"size": 14},
        "name": "Driver 4",
        "hoverinfo": "skip"
    })

    return (
        fig,
        metrics_panel("81", DRIVERS["81"], row_81),
        metrics_panel("4", DRIVERS["4"], row_4 )
    )


if __name__ == "__main__":
    app.run(debug=True)