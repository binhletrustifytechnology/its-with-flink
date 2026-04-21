import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parents[2] / ".env")

# Must be set before PyFlink initialises the JVM (Java 17+ module system requires these)
os.environ.setdefault("JAVA_TOOL_OPTIONS", " ".join([
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
]))

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment

VEHICLE_CLASS_MAP = {
    2: "bicycle",
    3: "motorbike",
    4: "car",
    6: "transporter",
    7: "short_truck",
    8: "long_truck",
}

VEHICLE_CLASSES = list(VEHICLE_CLASS_MAP.items())  # [(class_id, class_name), ...]


def build_aggregation_sql() -> str:
    """Build the pivot-style aggregation SQL for all vehicle classes."""
    select_parts = [
        "serial_number",
        "timestamp_seconds",
        "CAST(NULL AS STRING) AS direction",
        "SUM(vehicle_volume)  AS total",
    ]

    for class_id, class_name in VEHICLE_CLASSES:
        # Volume per class
        select_parts.append(
            f"CAST(SUM(CASE WHEN vehicle_class = {class_id} THEN vehicle_volume ELSE 0 END) AS INT)"
            f" AS {class_name}"
        )
        # Weighted average speed per class; 0.0 when no vehicles of that class
        select_parts.append(
            f"COALESCE("
            f"  SUM(CASE WHEN vehicle_class = {class_id} THEN vehicle_volume * vehicle_avg_speed END)"
            f"  / NULLIF(SUM(CASE WHEN vehicle_class = {class_id} THEN vehicle_volume ELSE 0 END), 0),"
            f"  0.0"
            f") AS {class_name}_speed"
        )

    cols = ",\n        ".join(select_parts)
    return f"""
        SELECT
        {cols}
        FROM vehicle_source
        GROUP BY serial_number, timestamp_seconds
    """


def _jdbc_url() -> str:
    host = os.environ.get("PG_HOST", "localhost")
    port = os.environ.get("PG_PORT", "5432")
    db   = os.environ.get("PG_DB",   "its")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def run(csv_path: str) -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)

    # ── Load JDBC JARs ────────────────────────────────────────────────────────
    lib_dir = Path(__file__).parents[2] / "lib"
    jar_uris = ";".join(
        f"file:///{jar.as_posix()}"
        for jar in lib_dir.glob("*.jar")
    )
    if jar_uris:
        env.add_jars(jar_uris)

    t_env = StreamTableEnvironment.create(env)

    # ── Source ────────────────────────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE vehicle_source (
            id                         STRING,
            serial_number              STRING,
            message_number             INT,
            timestamp_seconds_original STRING,
            timestamp_seconds          BIGINT,
            direction                  STRING,
            lane                       INT,
            vehicle_class              INT,
            vehicle_volume             INT,
            vehicle_avg_speed          DOUBLE,
            vehicle_85th_speed         DOUBLE,
            vehicle_occupancy          DOUBLE,
            headway                    DOUBLE,
            gap                        DOUBLE,
            creation_date              STRING,
            modification_date          STRING
        ) WITH (
            'connector'              = 'filesystem',
            'path'                   = '{csv_path}',
            'format'                 = 'csv',
            'csv.ignore-parse-errors' = 'true',
            'csv.null-literal'       = 'NULL'
        )
    """)

    # ── Sink (PostgreSQL via JDBC) ─────────────────────────────────────────────
    pg_user = os.environ.get("PG_USER", "postgres")
    pg_pass = os.environ.get("PG_PASSWORD", "postgres")

    t_env.execute_sql(f"""
        CREATE TABLE traffic_sink (
            serial_number     STRING,
            timestamp_seconds BIGINT,
            direction         STRING,
            total             INT,
            bicycle           INT,
            bicycle_speed     DOUBLE,
            motorbike         INT,
            motorbike_speed   DOUBLE,
            car               INT,
            car_speed         DOUBLE,
            transporter       INT,
            transporter_speed DOUBLE,
            short_truck       INT,
            short_truck_speed DOUBLE,
            long_truck        INT,
            long_truck_speed  DOUBLE
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{_jdbc_url()}',
            'table-name' = 'traffic_data',
            'driver'     = 'org.postgresql.Driver',
            'username'   = '{pg_user}',
            'password'   = '{pg_pass}'
        )
    """)

    # ── Execute ───────────────────────────────────────────────────────────────
    agg_sql = build_aggregation_sql()
    t_env.execute_sql(f"INSERT INTO traffic_sink {agg_sql}").wait()


if __name__ == "__main__":
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    csv_path = os.path.join(project_root, "resources", "vehicle_data.csv")
    run(csv_path)
