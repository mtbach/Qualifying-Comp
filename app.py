import pandas as pd
import numpy as np
import snowflake.connector
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, callback
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
METRICS = ["SPEED", "THROTTLE", "GEAR", "BRAKE"]

def load_session_list():
    query = "SELECT session_id, circuit_name from dim_openf1_api__sessions WHERE session_name = 'QUALIFYING' and year = 2025;"
    res = curr.execute(query).fetch_pandas_all()
    return res

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
            n_gear as gear, 
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


def row_at_progress(df, progress):
    idx = (df["PROGRESS"] - progress).abs().idxmin()
    return df.loc[idx]


session_list = load_session_list()
session_labels = dict(zip(session_list["SESSION_ID"], session_list["CIRCUIT_NAME"]))


def base_circuit_figure(TRACK_X, TRACK_Y):
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
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        height=300,
        uirevision="track",
        margin=dict(l=15, r=15, t=15, b=15)
    )

    return fig

def base_comparison_figure(df_81, df_4, metric):
    fig = go.Figure()

    # Driver 81
    fig.add_trace(
        go.Scatter(
            x=df_81["PROGRESS"],
            y=df_81[metric],
            mode="lines",
            name="Driver 81",
            line=dict(color="#e10600")
        )
    )

    # Driver 4
    fig.add_trace(
        go.Scatter(
            x=df_4["PROGRESS"],
            y=df_4[metric],
            mode="lines",
            name="Driver 4",
            line=dict(color="#00d2be")
        )
    )

    fig.update_layout(
        height=220,
        margin=dict(l=30, r=10, t=10, b=30),
        xaxis_title="Lap Progress",
        yaxis_title=metric,
        template="plotly_dark",
        legend=dict(orientation="h", y=1.1)
    )
    fig.update_xaxes(dtick=0.1)
    fig.update_yaxes(dtick=50)
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

def add_cursor(fig, progress):
    fig = go.Figure(fig)  
    fig.update_layout(
        shapes=[
            dict(
                type="line",
                x0=progress,
                x1=progress,
                y0=0,
                y1=1,
                xref="x",
                yref="paper",
                line=dict(color="white", width=2, dash="dash")
            )
        ]
    )

    return fig

def metrics_panel(driver, base_fig, row):

    return [{html.Div([

        dcc.Graph(
            id=f'{driver}-{metric}',
            figure=add_cursor(base_fig[metric], row["TIME"]),
            config={"displayModeBar": False},
        )
    ])} for metric in METRICS]


app = Dash(__name__)

app.layout = html.Div(
    children=[
    
        html.Div(
            id="metrics-left",
            children=[
                html.Div("DRIVER 1'S STATS GO HERE")
            ],
            style={
                "width": "20%",
                "padding": "16px",
                "backgroundColor": "#111",
                "color": "white",
                "borderRight": "1px solid #333"
            }
        ),

        html.Div([
            dcc.Dropdown(
                options=[
                    {'label': v, 'value': k}
                    for k, v in session_labels.items()
                ],
                value = 9689,
                id="session-dropdown",
                style={
                "width": "300px",
                "zIndex": 1000  
            }
            ),
            dcc.Graph(
                id="circuit",
                figure={},
                clear_on_unhover=False,
                style={"height": "500px", "width": "100%"}
            ),
            dcc.Graph(
                id="SPEED",
                style={"height": "200px", "width": "100%"}
            ),
            dcc.Graph(
                id="THROTTLE",
                style={"height": "200px", "width": "100%"}

            ),
            dcc.Graph(
                id="GEAR",
                style={"height": "200px", "width": "100%"}
            ),
            dcc.Graph(
                id="BRAKE",
                style={"height": "200px", "width": "100%"}

            )
        ],
            style={
                "width": "60%",
                "display": "flex",
                "flexDirection": "column",
                "alignItems": "center",
                "overflowY": "auto",
                "padding": "20px",
                "gap": "15px"
            }
        ),
         html.Div(
            id="metrics-right",
            children=[
                html.Div("DRIVER 2'S STATS GO HERE")
            ],
            style={
                "width": "20%",
                "padding": "16px",
                "backgroundColor": "#111",
                "color": "white",
                "borderRight": "1px solid #333"
            }
        ),

        dcc.Store(id="stored-progress"),
        dcc.Store(id="4-data"),
        dcc.Store(id="81-data")
    
    ],
    style={
        "display": "flex",
        "height": "100vh",
        "backgroundColor": "#0b0b0b",
        "fontFamily": "Arial, sans-serif"
    }
)


@callback(
    Output("circuit", "figure", allow_duplicate=True),
    Output("stored-progress", "data"),
    Output("SPEED", "figure", allow_duplicate=True),
    Output("THROTTLE", "figure", allow_duplicate=True),
    Output("GEAR", "figure", allow_duplicate=True),
    Output("BRAKE", "figure", allow_duplicate=True),

    Output("81-data", "data"),
    Output("4-data", "data"),
    Input("session-dropdown", "value"),
    prevent_initial_call=True
    )
def update_session(value):
    df_81 = load_session_data(value, 81)
    df_4 = load_session_data(value, 4)
    print(df_81)
    df_81 = preprocess_driver(df_81)
    df_4= preprocess_driver(df_4)

    TRACK_X = df_81["X"].values
    TRACK_Y = df_81["Y"].values
    TRACK_PROGRESS = df_81["PROGRESS"]
    json_progress = {
        "df": TRACK_PROGRESS.to_dict()
    }
    json_81 = {
        "df": df_81.to_dict()
    }
    json_4 = {
        "df": df_4.to_dict()
    }
    
    COMP_FIGS = {metric: base_comparison_figure(df_81, df_4, metric) for metric in METRICS}
    
    return (base_circuit_figure(TRACK_X, TRACK_Y), 
            json_progress, 
            COMP_FIGS['SPEED'],
            COMP_FIGS['THROTTLE'],
            COMP_FIGS['GEAR'],
            COMP_FIGS['BRAKE'],

            json_81,
            json_4)

@callback(
    Output("circuit", "figure"),
    Output("SPEED", "figure"),
    Output("THROTTLE", "figure"),
    Output("GEAR", "figure"),
    Output("BRAKE", "figure"),

    Input("circuit", "clickData"),
    Input("stored-progress", "data"),
    Input("81-data", "data"),
    Input("4-data", "data"),
    State("circuit", "figure"),
    State("SPEED", "figure"),
    State("THROTTLE", "figure"),
    State("GEAR", "figure"),
    State("BRAKE", "figure")
)
def scrub_track(clickData, TRACK_PROGRESS, DATA_81, DATA_4 , fig, speed, throttle, gear, brake):
    
    if clickData is None:
        raise PreventUpdate
    
    # unpacked_progress = pd.DataFrame(TRACK_PROGRESS["df"])
    unpacked_progress = pd.DataFrame(TRACK_PROGRESS)["df"].values
    unpacked_81 = pd.DataFrame(DATA_81["df"])
    unpacked_4 = pd.DataFrame(DATA_4["df"])
    
    point_idx = clickData["points"][0]["pointIndex"]
    
    progress = unpacked_progress[point_idx]
    print(progress)
    print(unpacked_4)
    row_81 = row_at_progress(unpacked_81, progress)
    row_4 = row_at_progress(unpacked_4, progress)

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
        add_cursor(speed, progress),
        add_cursor(throttle, progress),
        add_cursor(gear, progress),
        add_cursor(brake, progress)
    )


if __name__ == "__main__":
    app.run(debug=True)