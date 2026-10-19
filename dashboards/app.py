
from datetime import datetime

import pandas as pd
from dash import Dash, Input, Output, dcc, html, no_update

import charts

REFRESH_SECONDS = 5
BUILDERS = {key: build for key, _, build in charts.DASHBOARDS}

app = Dash(__name__, title="West City Bank dashboards")

app.layout = html.Div(style={"fontFamily": "sans-serif", "margin": "16px 24px"}, children=[
    html.H1("West City Bank: account opening dashboards", style={"marginBottom": "4px"}),
    html.Div(id="status", style={"color": "#555", "marginBottom": "12px"}),
    dcc.Tabs(id="tab", value=charts.DASHBOARDS[0][0],
             children=[dcc.Tab(label=title, value=key) for key, title, _ in charts.DASHBOARDS]),
    # Charts in a grid: two per row; charts marked wide use the whole row.
    html.Div(id="charts", style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "8px"}),
    dcc.Interval(id="tick", interval=REFRESH_SECONDS * 1000),       # the refresh clock
])


@app.callback(Output("status", "children"), Output("charts", "children"),
              Input("tab", "value"), Input("tick", "n_intervals"))
def refresh(tab, _tick):
    """Runs when the page opens, when a tab is clicked, and every
    REFRESH_SECONDS: read the tab's views and redraw its charts."""
    refreshed = f"{datetime.now():%H:%M:%S}"
    try:
        db = charts.connect()
        try:
            load = charts.view(db, "vw_last_load").iloc[0]
            figures = BUILDERS[tab](db)
        finally:
            db.close()
    except Exception as error:
        # e.g. the database is restarting: keep the charts on screen, retry.
        return f"Cannot read the warehouse right now ({error}); retrying every {REFRESH_SECONDS} s.", no_update

    if pd.isna(load["last_warehouse_load"]):
        loaded = "No warehouse load yet: run etl/run_all.sql"
    else:
        loaded = f"Warehouse last loaded {load['last_warehouse_load']:%d %b %H:%M:%S}"
    status = (f"{loaded} · {load['accounts_in_warehouse']} accounts in the warehouse · "
              f"page refreshed {refreshed} (every {REFRESH_SECONDS} s)")
    graphs = [dcc.Graph(id=f"{tab}-{i}", figure=fig,
                        style={"gridColumn": "1 / -1"} if fig.layout.meta == "wide" else {})
              for i, fig in enumerate(figures)]
    return status, graphs


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
