# Silsila: Agent-Based Smart Redistribution System

**Final Year Project, FAST-NUCES.** An AI-driven system that cuts waste of perishable FMCG goods by forecasting demand and redistributing near-expiry stock between retailers, with a B2C clearance marketplace for the rest.

![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white) ![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white) ![React](https://img.shields.io/badge/React-20232A?logo=react&logoColor=61DAFB) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white) ![Prophet](https://img.shields.io/badge/Prophet-forecasting-blue) ![Mesa](https://img.shields.io/badge/Mesa-multi--agent-green)

## System
- **Forecasting:** Facebook Prophet on historical daily sales, per client and product
- **Decision engine:** multi-agent simulation (Mesa) that flags expiry-risk anomalies and generates constraint-based redistribution recommendations
- **Backend:** FastAPI REST APIs for inventory, batches and expiry tracking, redistribution workflows and eco-points
- **Data model:** normalised PostgreSQL schema with foreign keys and transactional consistency
- **Frontend:** React dashboard and clearance marketplace

## This repository
Contains the data layer of the project:

| File | Purpose |
|---|---|
| `datasetGeneration.py` | Generates synthetic clients, distributor products, daily sales and stock batches |
| `preprocessing.py` | Cleans and processes the grocery inventory and sales dataset |
| `stocknormcalculation.py` | Calculates stock norms and allocation per client |
| `data/` | Generated datasets (10 clients, daily sales, batches, stock norms) |

```bash
pip install -r requirements.txt
python datasetGeneration.py
python preprocessing.py
python stocknormcalculation.py
```
