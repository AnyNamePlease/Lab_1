# Lab_1
## Ответы для lab1
``` sql
SELECT *
FROM flights
WHERE arrival_airport = 'SVO'
  AND COALESCE(actual_arrival, scheduled_arrival)::date = '2017-07-22'
  AND COALESCE(actual_arrival, scheduled_arrival)::time BETWEEN '16:00' AND '19:00'
ORDER BY COALESCE(actual_arrival, scheduled_arrival) ASC;
```

``` sql
UPDATE ticket_flights
SET amount = amount + 1000
WHERE flight_id IN (
    SELECT flight_id
    FROM flights
    WHERE (COALESCE(actual_departure, scheduled_departure)::time BETWEEN '07:00' AND '10:00')
       OR (COALESCE(actual_departure, scheduled_departure)::time BETWEEN '17:00' AND '20:00')
);
```

``` sql
SELECT sub.timezone,
       sub.airport_code,
       sub.airport_name->>'en' AS airport_name_en,
       sub.airport_name->>'ru' AS airport_name_ru,
       sub.city->>'en' AS city_en,
       sub.city->>'ru' AS city_ru,
       sub.coordinates[1] AS latitude,   -- широта
       sub.coordinates[0] AS longitude   -- долгота
FROM (
    SELECT a.*,
           ROW_NUMBER() OVER (
               PARTITION BY a.timezone 
               ORDER BY a.coordinates[1] DESC  -- сортировка по широте
           ) AS rn
    FROM airports_data a
) AS sub
WHERE rn = 1
ORDER BY timezone;
```
## Ответ для lab2
``` sql
WITH passenger_segments AS (
    SELECT
        t.passenger_id,
        f.departure_airport,
        f.arrival_airport,
        COALESCE(f.actual_departure, f.scheduled_departure) AS dep_time,
        COALESCE(f.actual_arrival, f.scheduled_arrival)     AS arr_time
    FROM bookings.tickets t
    JOIN bookings.ticket_flights tf ON t.ticket_no = tf.ticket_no
    JOIN bookings.flights f         ON tf.flight_id = f.flight_id
    WHERE f.scheduled_departure BETWEEN '2017-07-01' AND '2017-08-01'
),

ordered AS (
    SELECT
        passenger_id,
        departure_airport,
        arrival_airport,
        arr_time,
        LEAD(dep_time) OVER (PARTITION BY passenger_id ORDER BY dep_time) AS next_dep,
        LEAD(departure_airport) OVER (PARTITION BY passenger_id ORDER BY dep_time) AS next_origin
    FROM passenger_segments
),

connections AS (
    SELECT
        passenger_id,
        arrival_airport AS hub_airport,
        EXTRACT(EPOCH FROM (next_dep - arr_time))/60 AS conn_min
    FROM ordered
    WHERE next_origin = arrival_airport
      AND next_dep > arr_time
      AND (next_dep - arr_time) BETWEEN INTERVAL '20 minutes' AND INTERVAL '24 hours'
),

transit_stats AS (
    SELECT
        hub_airport AS airport,
        COUNT(DISTINCT passenger_id) AS transit_passengers,
        ROUND(AVG(conn_min),1) AS avg_conn_min
    FROM connections
    GROUP BY hub_airport
),

total_stats AS (
    SELECT airport, COUNT(DISTINCT passenger_id) AS total_passengers
    FROM (
        SELECT passenger_id, departure_airport AS airport FROM passenger_segments
        UNION ALL
        SELECT passenger_id, arrival_airport AS airport FROM passenger_segments
    ) all_visits
    GROUP BY airport
)

SELECT
    ts.airport,
    ad.airport_name ->> 'en' AS airport_name,
    COALESCE(tot.total_passengers, 0) AS total_passengers,
    COALESCE(ts.transit_passengers, 0) AS transit_passengers,
    ROUND(
        CASE WHEN tot.total_passengers > 0
             THEN ts.transit_passengers::numeric / tot.total_passengers
             ELSE 0 END, 4
    ) AS transit_ratio,
    ts.avg_conn_min
FROM transit_stats ts
JOIN bookings.airports_data ad ON ad.airport_code = ts.airport
LEFT JOIN total_stats tot ON tot.airport = ts.airport
ORDER BY transit_ratio DESC
LIMIT 15;
```
