import snowflake.connector


class OpenF1DataAccess:
    def __init__(self, snowflake_config):
        self.conn = snowflake.connector.connect(**snowflake_config)
        self.curr = self.conn.cursor()

    def get_sessions(self, year=2025, session_name="QUALIFYING"):
        query = """
            SELECT session_id, circuit_name
            FROM dim_openf1_api__sessions
            WHERE session_name = %s
              AND year = %s
            ORDER BY session_id
        """
        return self.curr.execute(query, (session_name, year)).fetch_pandas_all()

    def get_fastest_lap_window(self, session_id, driver_number):
        query = """
            SELECT
                time_start,
                timeadd(second, lap_duration, time_start) AS time_end
            FROM fct_openf1_api__fastest_lap
            WHERE session_id = %s
              AND driver_number = %s
        """
        res = self.curr.execute(query, (session_id, driver_number)).fetchone()
        if res is None:
            raise ValueError(
                f"No fastest lap window found for session_id={session_id}, driver_number={driver_number}"
            )
        return res[0], res[1]

    def get_driver_telemetry_for_window(self, session_id, driver_number, time_start, time_end):
        driver_telemetry_table = {
            4: "fct_openf1_api__telemetry_4",
            81: "fct_openf1_api__telemetry_81",
        }.get(int(driver_number))

        if driver_telemetry_table is None:
            raise ValueError(f"Unsupported driver_number={driver_number}")

        query = f"""
            SELECT
                driver_number,
                time,
                x,
                y,
                speed,
                rpm,
                n_gear AS gear,
                throttle,
                brake
            FROM {driver_telemetry_table}
            WHERE session_id = %s
              AND time >= to_timestamp_ntz(%s)
              AND time <= to_timestamp_ntz(%s)
            ORDER BY time
        """

        return self.curr.execute(query, (session_id, time_start, time_end)).fetch_pandas_all()

    def get_fastest_lap_metadata(self, session_id, driver_number):
        query = """
            SELECT
                session_id,
                to_varchar(time_start, 'YYYY-MM-DD HH24:MI:SS.FF3') AS time_start,
                lap_duration,
                duration_sector_1,
                duration_sector_2,
                duration_sector_3,
                compound,
                tyre_age
            FROM fct_openf1_api__fastest_lap
            WHERE session_id = %s
              AND driver_number = %s
        """
        return self.curr.execute(query, (session_id, driver_number)).fetch_pandas_all()
