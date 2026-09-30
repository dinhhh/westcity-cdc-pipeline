# =============================================================================
# Python image for the Kafka consumers (consumers/consumer.py) and the
# dashboards (dashboards/app.py web app, dashboards/build_dashboards.py).
# Same base image as the course lab (advanced-database-lab/python/Dockerfile):
# python:3.11-slim. The libraries ship as ready-built wheels, so no
# compilers or system packages are needed.
#   psycopg2-binary  PostgreSQL client
#   confluent-kafka  Kafka client
#   pandas           holds query results for the charts
#   plotly           draws the charts
#   dash             serves them as a live web app
# =============================================================================
FROM python:3.11-slim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /workspace
CMD ["bash"]
