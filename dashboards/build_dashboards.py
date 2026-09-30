"""
dashboards/build_dashboards.py - save the three dashboards as HTML files.

Run it after a warehouse load (etl/run_all.sql):
    docker compose exec python python /workspace/dashboards/build_dashboards.py

Output (re-running the script refreshes them):
    dashboards/output/d1_account_opening_growth.html
    dashboards/output/d2_onboarding_risk_compliance.html
    dashboards/output/d3_customer_engagement_consistency.html

The same charts are shown live, and refresh by themselves, in the web app
dashboards/app.py (http://localhost:8050). Both use dashboards/charts.py.
"""
import os
from datetime import datetime

import charts

OUTPUT_DIR = "/workspace/dashboards/output"


def write_dashboard(filename, title, figures):
    """Put the figures one under the other in a single HTML file. Plotly's
    JavaScript is embedded once, so the file also opens offline."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    parts = [f"<html><head><meta charset='utf-8'><title>{title}</title></head>",
             "<body style='font-family: sans-serif; margin: 24px'>",
             f"<h1>{title}</h1>",
             f"<p>West City Bank &middot; built {datetime.now():%d %b %Y %H:%M} "
             f"from the gold views in westcity_dw</p>"]
    for i, fig in enumerate(figures):
        # default_height: charts without their own height get 480 px
        # (otherwise each one would fill the whole browser window).
        parts.append(fig.to_html(full_html=False, include_plotlyjs=(i == 0),
                                 default_height="480px"))
    parts.append("</body></html>")
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(parts))
    print(f"wrote {path}")


db = charts.connect()
for key, title, build in charts.DASHBOARDS:
    write_dashboard(f"{key}.html", title, build(db))
db.close()
