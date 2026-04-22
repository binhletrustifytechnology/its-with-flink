import os

# Must be set before PyFlink initialises the JVM (Java 17+ module system requires these)
os.environ.setdefault("JAVA_TOOL_OPTIONS", " ".join([
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
]))

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

VEHICLE_CLASS_MAP = {
    2: "bicycle",
    3: "motorbike",
    4: "car",
    6: "transporter",
    7: "short_truck",
    8: "long_truck",
}

VEHICLE_CLASSES = list(VEHICLE_CLASS_MAP.items())


def build_aggregation_sql() -> str:
    select_parts = [
        "serial_number",
        "timestamp_seconds",
        "CAST(NULL AS STRING) AS direction",
        "SUM(vehicle_volume) AS total",
    ]

    for class_id, class_name in VEHICLE_CLASSES:
        select_parts.append(
            f"CAST(SUM(CASE WHEN vehicle_class = {class_id} THEN vehicle_volume ELSE 0 END) AS INT)"
            f" AS {class_name}"
        )
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


def _pg_opts() -> dict:
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "db": os.environ.get("POSTGRES_DB", "its"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
    }


def _kafka_opts() -> dict:
    return {
        "bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "topic": os.environ.get("KAFKA_TOPIC", "topic-sensors"),
        "group_id": os.environ.get("KAFKA_GROUP_ID", "vehicle_aggregation_group"),
        "startup_mode": os.environ.get("KAFKA_STARTUP_MODE", "earliest-offset"),
    }


def run() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(env)

    pg = _pg_opts()
    kafka = _kafka_opts()
    jdbc_url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['db']}"

    # ── Source: Kafka (JSON) ───────────────────────────────────────────────────
    # event_time converts timestamp_seconds (Unix s) to TIMESTAMP_LTZ for watermarking
    t_env.execute_sql(f"""
        CREATE TABLE vehicle_source (
            id                         STRING,
            serial_number              STRING,
            message_number             INT,
            timestamp_seconds_original INT,
            timestamp_seconds          INT,
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
            modification_date          STRING,
            event_time AS TO_TIMESTAMP_LTZ(CAST(timestamp_seconds AS BIGINT) * 1000, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector'                = 'kafka',
            'topic'                    = '{kafka["topic"]}',
            'properties.bootstrap.servers' = '{kafka["bootstrap_servers"]}',
            'properties.group.id'      = '{kafka["group_id"]}',
            'scan.startup.mode'        = '{kafka["startup_mode"]}',
            'format'                   = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # ── Sink: PostgreSQL JDBC (upsert via primary key) ─────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE traffic_sink (
            serial_number     STRING,
            timestamp_seconds INT,
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
            long_truck_speed  DOUBLE,
            PRIMARY KEY (serial_number, timestamp_seconds) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{jdbc_url}',
            'table-name' = 'traffic_data',
            'driver'     = 'org.postgresql.Driver',
            'username'   = '{pg["user"]}',
            'password'   = '{pg["password"]}'
        )
    """)

    # ── Execute ───────────────────────────────────────────────────────────────
    agg_sql = build_aggregation_sql()
    t_env.execute_sql(f"INSERT INTO traffic_sink {agg_sql}").wait()


if __name__ == "__main__":
    run()
