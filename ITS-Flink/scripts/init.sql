CREATE TABLE traffic_v2 (
    serial_number     VARCHAR,
    timestamp_seconds BIGINT,
    direction         VARCHAR,
    total             INT,
    bicycle           INT,
    bicycle_speed     DOUBLE PRECISION,
    motorbike         INT,
    motorbike_speed   DOUBLE PRECISION,
    car               INT,
    car_speed         DOUBLE PRECISION,
    transporter       INT,
    transporter_speed DOUBLE PRECISION,
    short_truck       INT,
    short_truck_speed DOUBLE PRECISION,
    long_truck        INT,
    long_truck_speed  DOUBLE PRECISION
);