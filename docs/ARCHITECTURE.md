# Repository Architecture and Data Flow

## High-level architecture

This project has two layers:

1. **Warehouse modeling (dbt + Snowflake)**
   - Raw OpenF1 API tables in `raw.openf1_api` are declared as dbt sources.
   - Staging models standardize names/types.
   - Mart models produce dashboard-ready dimension/fact tables.

2. **Application layer (Dash + Plotly + Snowflake connector)**
   - A Dash app queries marts directly in Snowflake.
   - It builds per-session fastest-lap comparison visuals for drivers **81** and **4**.

## dbt model graph

### Sources
Declared in `models/staging/openf1_api/_src_openf1_api.yml`:
- sessions
- session_result
- fp3_result
- laps
- stint
- drivers
- car_telemetry
- car_location

### Staging models (`stg_openf1_api__*`)
Each staging model maps source fields to standardized names (e.g., `session_key -> session_id`, `date -> time`) and normalizes some text with `upper(...)`.

### Mart models
- `dim_openf1_api__sessions`
  - Session metadata dimension used to populate the dashboard dropdown.
- `fct_openf1_api__fastest_lap`
  - Computes each driver's fastest lap per session (qualifying/practice), enriching with tire compound and tire age.
- `fct_openf1_api__telemetry_81` and `fct_openf1_api__telemetry_4`
  - Driver-specific telemetry+location facts by joining car telemetry and car location on timestamp.

## Runtime data flow in `app.py`

1. **Startup / initialization**
   - Environment variables are loaded from `.env`.
   - A Snowflake connection and cursor are created.
   - `load_session_list()` queries `dim_openf1_api__sessions` for `session_name = 'QUALIFYING'` and `year = 2025`.

2. **Session change callback (`update_session`)**
   - For selected session id, fetches fastest-lap time window from `fct_openf1_api__fastest_lap` for each driver.
   - Pulls telemetry/location rows from each driver-specific telemetry fact constrained to that lap window.
   - Preprocesses both dataframes to compute cumulative distance and normalized lap progress (`PROGRESS`).
   - Builds baseline figures:
     - Circuit map with sector coloring.
     - Overlay line charts for `SPEED`, `THROTTLE`, `GEAR`, `BRAKE`.
   - Stores progress and driver telemetry in `dcc.Store` for interactive scrubbing.

3. **Track click callback (`scrub_track`)**
   - Reads clicked track point index.
   - Converts click index to lap progress.
   - Adds a marker on the circuit at the selected progress point.
   - Adds a vertical cursor line at the same progress on all comparison charts.

## Design notes and constraints

- **Hardcoded season and drivers:** app logic currently targets year `2025` and drivers `81` and `4`.
- **Data access layer:** `app.py` now delegates Snowflake reads to `OpenF1DataAccess` (`data_access.py`) to centralize query logic outside UI callbacks.
- **Warehouse assumptions:** mart models and app both assume Snowflake warehouse `compute_wh` and the dbt model names exist.
- **Potential join risk:** telemetry/location marts join only on `time`; if timestamps overlap across sessions, joins could mix rows unless `session_id` is included in join keys.

## End-to-end pipeline summary

OpenF1 raw tables -> dbt sources -> dbt staging models -> dbt marts (`dim_*`, `fct_*`) -> Dash app data-access layer (`OpenF1DataAccess`) -> pandas preprocessing (`PROGRESS`) -> Plotly figures -> interactive scrubbing callbacks.
