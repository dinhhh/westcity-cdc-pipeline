import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from plotly.subplots import make_subplots

# Read SQL numeric values (e.g. percentages) as Python floats, so the charts
# treat them as numbers. This converts the type only; it calculates nothing.
psycopg2.extensions.register_type(psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values, "NUMERIC_AS_FLOAT",
    lambda value, cursor: float(value) if value is not None else None))


def connect():
    """A new connection to the warehouse."""
    return psycopg2.connect(host="postgres", port=5432, dbname="westcity_dw",
                            user="westcity_admin", password="lab_admin")


def view(db, name):
    """SELECT * FROM gold.<name>, returned as a pandas DataFrame."""
    with db.cursor() as cur:
        cur.execute(f"SELECT * FROM gold.{name}")
        return pd.DataFrame(cur.fetchall(), columns=[col.name for col in cur.description])


# -----------------------------------------------------------------------------
# Chart helpers: each takes one view's rows and returns a Plotly figure.
# -----------------------------------------------------------------------------

def kpis(df, title):
    """One big number per column of a one-row view."""
    fig = go.Figure()
    for i, column in enumerate(df.columns):
        fig.add_trace(go.Indicator(mode="number", value=df[column].iloc[0],
                                   title={"text": column.replace("_", " ")},
                                   domain={"row": 0, "column": i}))
    fig.update_layout(title=title, grid={"rows": 1, "columns": len(df.columns)}, height=250)
    return fig


def table(df, title):
    """The view's rows as a table."""
    fig = go.Figure(go.Table(header={"values": list(df.columns)},
                             cells={"values": [df[column] for column in df.columns]}))
    fig.update_layout(title=title, height=520)
    return fig


def counts_and_pct(df, x, counts, pct, title, bars=False, bar_labels=None):
    """Counts on the left axis (lines, or bars if bars=True, optionally
    labelled with another column) and a percentage line on the right axis."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    for column in counts:
        if bars:
            trace = go.Bar(x=df[x], y=df[column], name=column.replace("_", " "),
                           text=df[bar_labels] if bar_labels else None)
        else:
            trace = go.Scatter(x=df[x], y=df[column], name=column.replace("_", " "))
        fig.add_trace(trace, secondary_y=False)
    fig.add_trace(go.Scatter(x=df[x], y=df[pct], name=pct.replace("_", " "), mode="lines+markers"),
                  secondary_y=True)
    fig.update_yaxes(title_text="%", range=[0, 105], secondary_y=True)
    fig.update_layout(title=title)
    return fig


def wide(fig):
    """Mark a chart to use the full page width in the web app (KPIs, tables,
    time lines); the others share a row, two per row."""
    fig.update_layout(meta="wide")
    return fig


def keep_view_state(figures):
    """In the web app the charts are redrawn on every refresh. uirevision
    tells Plotly to keep the user's zoom and hidden legend items while the
    chart's title stays the same."""
    for fig in figures:
        fig.update_layout(uirevision=fig.layout.title.text)
    return figures


# -----------------------------------------------------------------------------
# The three dashboards: each function reads its views and returns its charts.
# -----------------------------------------------------------------------------

def d1_account_opening_growth(db):
    """D1 Account Opening Growth (business; UC1)."""
    return keep_view_state([
        wide(kpis(view(db, "vw_d1_kpi_today"), "Today")),
        wide(px.line(view(db, "vw_d1_daily_openings"), x="open_date", y=["openings", "approved"],
                     labels={"value": "accounts", "variable": ""},
                     title="Daily openings, last 90 days")),
        px.bar(view(db, "vw_d1_openings_by_product_channel"), x="product", y="openings",
               color="channel", barmode="group", title="Openings by product and channel"),
        px.bar(view(db, "vw_d1_openings_by_segment"), x="segment", y="openings", text="pct",
               title="Openings by customer segment (label = % of all openings)"),
        px.bar(view(db, "vw_d1_openings_by_state"), x="state", y="openings",
               title="Openings by state"),
        wide(table(view(db, "vw_d1_latest_openings"), "Latest 15 openings")),
    ])


def d2_onboarding_risk_compliance(db):
    """D2 Onboarding Risk & Compliance (UC2)."""
    return keep_view_state([
        px.bar(view(db, "vw_d2_decision_mix"), x="channel", y="accounts", color="risk_decision",
               barmode="stack", title="Fraud decisions by channel"),
        wide(px.bar(view(db, "vw_d2_risk_band_weekly"), x="week", y="accounts", color="risk_band",
                    barmode="stack", title="Risk bands per opening week")),
        px.bar(view(db, "vw_d2_shared_device_rings"), x="device_id",
               y=["customers_on_device", "approved", "declined"], barmode="group",
               labels={"value": "customers", "variable": ""},
               title="Devices shared by 3+ customers (possible fraud rings)"),
        counts_and_pct(view(db, "vw_d2_time_to_decision"), "week", ["avg_minutes_to_decision"],
                       "pct_within_5_min", "Time from opening to fraud decision, per week"),
        px.bar(view(db, "vw_d2_top_reason_codes"), x="reason_code", y="occurrences",
               title="Top risk reason codes"),
    ])


def d3_customer_engagement_consistency(db):
    """D3 Customer Engagement & Consistency (UC3)."""
    return keep_view_state([
        px.bar(view(db, "vw_d3_loyalty_enrolment_by_segment"), x="segment",
               y=["approved_accounts", "enrolled"], barmode="group", text_auto=True,
               labels={"value": "accounts", "variable": ""},
               title="Loyalty enrolment of approved accounts, by segment"),
        counts_and_pct(view(db, "vw_d3_welcome_offers_monthly"), "month", ["points_committed"],
                       "redeemed_pct", "Welcome offers per month: points committed (bars, "
                       "label = offers issued) and % redeemed (line)",
                       bars=True, bar_labels="offers_issued")
            .update_xaxes(dtick="M1", tickformat="%b %Y"),                # one label per month
        counts_and_pct(view(db, "vw_d3_welcome_call_coverage"), "week",
                       ["approved_accounts", "welcome_tasks"], "completed_pct",
                       "Welcome-call coverage per opening week"),
        wide(table(view(db, "vw_d3_consistency_checks"), "Consistency checks across systems (all should be 0)")),
    ])


# (key, title, function) of every dashboard, in display order.
DASHBOARDS = [
    ("d1_account_opening_growth",          "D1 Account Opening Growth",            d1_account_opening_growth),
    ("d2_onboarding_risk_compliance",      "D2 Onboarding Risk & Compliance",      d2_onboarding_risk_compliance),
    ("d3_customer_engagement_consistency", "D3 Customer Engagement & Consistency", d3_customer_engagement_consistency),
]
