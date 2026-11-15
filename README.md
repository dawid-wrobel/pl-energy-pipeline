# pl-energy-pipeline

Automated ELT pipeline for the Polish power system. It collects renewable
energy production, electricity prices and weather data, models them in an
analytical warehouse and shows how wind/solar output and prices depend on
the weather.

> Status: work in progress.

## Tech stack

Python 3.12 · DuckDB · Parquet · dbt (dbt-duckdb) · Dagster · Streamlit ·
Docker Compose · GitHub Actions (ruff, pytest)

## Architecture

Diagram and design decisions (ADRs) will be added as the project grows.

## Data sources and licences

This project processes data from the following sources. The data was
transformed (time normalisation to UTC, aggregation, joining) and is not
endorsed by the providers. Check each provider's current terms before reuse.

- **PSE S.A.** - Polish Power Grid Operator, https://api.raporty.pse.pl.
  Source: PSE. The retrieval date of every record is stored in the raw layer
  (`ingested_at`) and shown in the dashboard footer. The PSE logo is not used.
- **Energy-Charts** - Fraunhofer ISE, https://energy-charts.info, licensed
  under CC BY 4.0. Attribution: "energy-charts.info".
- **Open-Meteo** - weather data by https://open-meteo.com, licensed under
  CC BY 4.0, free for non-commercial use.

## License

Code: MIT, see [LICENSE](LICENSE). Data remains under the licences of the
providers listed above.