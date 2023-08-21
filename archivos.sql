---CREACION DE STREAM----
CREATE STREAM data_trade_stream (
    price DOUBLE,
    symbol STRING,
    timestamp STRING,
    volume DOUBLE
) WITH (
    KAFKA_TOPIC='stock-update',
    VALUE_FORMAT='JSON'
);

---VISTA 1----
CREATE TABLE promedio_ponderado AS
    SELECT symbol,
           SUM(price * volume) / SUM(volume) AS promedio_ponderado
    FROM data_trade_stream
    GROUP BY symbol
    EMIT CHANGES;

SELECT * FROM promedio_ponderado;

-----VISTA 2-----
CREATE TABLE cantidad_transacciones AS
    SELECT symbol, COUNT(*) AS total_transacciones
    FROM data_trade_stream
    GROUP BY symbol
    EMIT CHANGES;

SELECT * FROM cantidad_transacciones;

--VISTA 3----
CREATE TABLE precio_maximo AS
    SELECT symbol, MAX(price) AS max_price
    FROM data_trade_stream
    GROUP BY symbol
    EMIT CHANGES;

SELECT * FROM precio_maximo;

--VISTA 4----
CREATE TABLE precio_minimo AS
    SELECT symbol, MIN(price) AS min_price
    FROM data_trade_stream
    GROUP BY symbol
    EMIT CHANGES;

SELECT * FROM precio_minimo;
