# ⚡ Energy Data Pipeline – RTE France Energy Consumption Data Ingestion

This project is a simple but production-ready **data engineering pipeline** that fetches **yearly electricity consumption data** from the **RTE (Réseau de Transport d'Électricité)** API and stores it in a **PostgreSQL database located in a docker container**. 

It’s fully containerized with **Docker** and easy to extend for forecasting, visualization, or real-time streaming.

The goal of this project is to show how we can do a simple ingestion from an API to a portgress database and visualise the result with adminer.

---

## 🧱 Project Overview

![Pipeline Diagram](RTE---Energy-Consumption-/Pipelines design.drawio.drawio.png)

- **Data Source:** RTE Open Data – [national-tr API](https://data.rte-france.com)
- **Pipeline Steps:**
  1. Authenticate with the RTE API using OAuth2.
  2. Fetch yearly electricity consumption from 2015 to 2025.
  3. Setup a docker container with the requirement to run the project (Postgres,Adminer).
  4. Establish the connection to the API we subscribe.
  5. Fetch the data from an API call and get JSON reponse.
  6. Store the results in a PostgreSQL database located in the container .
  7. Validate the result with adminer.
  
- **Technologies Used:**
  - Python (requests, psycopg2)
  - PostgreSQL
  - Docker + Docker Compose
  - Adminer (web DB viewer)

---
