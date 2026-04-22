CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE traffic_v2
(
    id                     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    device_id              UUID,
    device_name            VARCHAR(50),
    serial_number          VARCHAR(50),
    timestamp_seconds      BIGINT,
    timestamp_milliseconds BIGINT,
    direction              VARCHAR(50),
    lane                   INTEGER,

    total                  INT,
    short_truck            INT,
    long_truck             INT,
    car                    INT,
    motorbike              INT,
    bicycle                INT,
    transporter            INT,
    unknown                INT,

    short_truck_speed      DOUBLE PRECISION,
    short_truck_85th_speed DOUBLE PRECISION,
    short_truck_occupancy  DOUBLE PRECISION,

    long_truck_speed       DOUBLE PRECISION,
    long_truck_85th_speed  DOUBLE PRECISION,
    long_truck_occupancy   DOUBLE PRECISION,

    car_speed              DOUBLE PRECISION,
    car_85th_speed         DOUBLE PRECISION,
    car_occupancy          DOUBLE PRECISION,
    motorbike_speed        DOUBLE PRECISION,
    motorbike_85th_speed   DOUBLE PRECISION,
    motorbike_occupancy    DOUBLE PRECISION,

    bicycle_speed          DOUBLE PRECISION,
    bicycle_85th_speed     DOUBLE PRECISION,
    bicycle_occupancy      DOUBLE PRECISION,

    transporter_speed      DOUBLE PRECISION,
    transporter_85th_speed DOUBLE PRECISION,
    transporter_occupancy  DOUBLE PRECISION,

    unknown_speed          DOUBLE PRECISION,
    unknown_85th_speed     DOUBLE PRECISION,
    unknown_occupancy      DOUBLE PRECISION,

    gap                    DOUBLE PRECISION,
    headway                DOUBLE PRECISION,

    creation_date          TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,
    modification_date      TIMESTAMP        DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE traffic_v2 ADD CONSTRAINT traffic_v2_serial_ts_uniq UNIQUE (serial_number, timestamp_seconds);
