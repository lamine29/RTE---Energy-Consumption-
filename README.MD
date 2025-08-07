# ⚡ Energy Data Pipeline – RTE Eco2mix Ingestion

This project is a simple but production-ready **data engineering pipeline** that fetches **daily electricity consumption data** from the **RTE (Réseau de Transport d'Électricité)** Eco2mix API and stores it in a **PostgreSQL database**. It’s fully containerized with **Docker** and easy to extend for forecasting, visualization, or real-time streaming.

The goal of this project is to show how we can do a simple ingestion from an API to a local portgress database.

---

## 🧱 Project Overview

![Pipeline Diagram](RTE---Energy-Consumption-/Pipelines design.drawio.drawio.png)

- **Data Source:** RTE Open Data – [Eco2mix national-tr API](https://data.rte-france.com)
- **Pipeline Steps:**
  1. Authenticate with the RTE API using OAuth2.
  2. Fetch daily electricity production (nuclear, wind, solar, hydro).
  3. Store the results in a PostgreSQL database.
- **Technologies Used:**
  - Python (requests, psycopg2)
  - PostgreSQL
  - Docker + Docker Compose
  - Adminer (web DB viewer)

---
