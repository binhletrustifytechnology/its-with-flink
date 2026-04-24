import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def _pg_opts() -> dict:
    return {
        "host":     os.environ.get("POSTGRES_HOST",     "localhost"),
        "port":     os.environ.get("POSTGRES_PORT",     "5432"),
        "db":       os.environ.get("POSTGRES_DB",       "its"),
        "user":     os.environ.get("POSTGRES_USER",     "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
    }


def _kafka_opts() -> dict:
    return {
        "bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "topic":             os.environ.get("KAFKA_TOPIC",             "topic-sensors-2"),
        "group_id":          os.environ.get("KAFKA_RAW_GROUP_ID",     "vehicle_raw_sink_group"),
        "startup_mode":      os.environ.get("KAFKA_STARTUP_MODE",     "latest-offset"),
    }


# ── Environments ──────────────────────────────────────────────────────────────
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
settings = EnvironmentSettings.in_streaming_mode()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

CHECKPOINT_DIR = os.getenv(
    "FLINK_CHECKPOINT_DIR",
    "file:///opt/flink/checkpoints"   # adjust to any writable local path
)

# ── Apply the chosen backend ──────────────────────────────────────────────────
_rocksdb_backend = EmbeddedRocksDBStateBackend()
env.set_state_backend(_rocksdb_backend)
env.get_checkpoint_config().set_checkpoint_storage(
    FileSystemCheckpointStorage(CHECKPOINT_DIR)
)
# Checkpointing: every 30 s, exactly-once, 3 retained checkpoints
env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_min_pause_between_checkpoints(5_000)
env.get_checkpoint_config().set_checkpoint_timeout(60_000)
env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(3)

pg    = _pg_opts()
kafka = _kafka_opts()
jdbc_url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['db']}"

# ── Source: Kafka (JSON) ──────────────────────────────────────────────────────
t_env.execute_sql(f"""
    CREATE TABLE vehicle_source (
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
        proc_time  AS PROCTIME(),
        event_time AS TO_TIMESTAMP_LTZ(CAST(timestamp_seconds AS BIGINT) * 1000, 3),
        WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = '{kafka["topic"]}',
        'properties.bootstrap.servers' = '{kafka["bootstrap_servers"]}',
        'properties.group.id'          = '{kafka["group_id"]}',
        'scan.startup.mode'            = '{kafka["startup_mode"]}',
        'format'                       = 'json',
        'json.ignore-parse-errors'     = 'true'
    )
""")

# ── Sink: vehicle_data PostgreSQL table (upsert on id) ───────────────────────
t_env.execute_sql(f"""
    CREATE TABLE vehicle_raw_sink (
        serial_number     STRING,
        message_number    INT,
        timestamp_seconds INT,
        direction         STRING,
        lane              INT,
        vehicle_class     INT,
        vehicle_volume    INT,
        vehicle_avg_speed  DOUBLE,
        vehicle_85th_speed DOUBLE,
        vehicle_occupancy  DOUBLE,
        headway            DOUBLE,
        gap                DOUBLE,
        PRIMARY KEY (serial_number, timestamp_seconds, message_number) NOT ENFORCED
    ) WITH (
        'connector'  = 'jdbc',
        'url'        = '{jdbc_url}',
        'table-name' = 'vehicle_data_v2',
        'driver'     = 'org.postgresql.Driver',
        'username'   = '{pg["user"]}',
        'password'   = '{pg["password"]}'
    )
""")

# ── Deduplicate on id then insert raw ─────────────────────────────────────────
statement_set = t_env.create_statement_set()
statement_set.add_insert_sql("""
    INSERT INTO vehicle_raw_sink
    SELECT
        serial_number,
        message_number,
        timestamp_seconds,
        direction,
        lane,
        vehicle_class,
        vehicle_volume,
        vehicle_avg_speed,
        vehicle_85th_speed,
        vehicle_occupancy,
        headway,
        gap
    FROM (
         SELECT 
             *,
             ROW_NUMBER() OVER (PARTITION BY serial_number, timestamp_seconds, message_number ORDER BY proc_time DESC) AS row_num
         FROM vehicle_source
     )
    WHERE row_num = 1
""")

statement_set.execute()
