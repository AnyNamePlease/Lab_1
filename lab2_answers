WITH passenger_segments AS (
    -- каждый билет + рейс = один перелёт конкретного пассажира
    SELECT
        t.passenger_id,
        t.ticket_no,
        t.book_ref,
        tf.flight_id,
        f.departure_airport,
        f.arrival_airport,
        COALESCE(f.actual_departure, f.scheduled_departure) AS dep_time,
        COALESCE(f.actual_arrival, f.scheduled_arrival)     AS arr_time
    FROM bookings.tickets t
    JOIN bookings.ticket_flights tf ON t.ticket_no = tf.ticket_no
    JOIN bookings.flights f         ON tf.flight_id = f.flight_id
),

ordered_segments AS (
    -- упорядочиваем сегменты маршрута каждого пассажира
    SELECT
        passenger_id,
        ticket_no,
        book_ref,
        flight_id,
        departure_airport,
        arrival_airport,
        dep_time,
        arr_time,
        LEAD(departure_airport) OVER (PARTITION BY passenger_id, book_ref ORDER BY dep_time) AS next_origin,
        LEAD(arrival_airport)   OVER (PARTITION BY passenger_id, book_ref ORDER BY dep_time) AS next_dest,
        LEAD(dep_time)          OVER (PARTITION BY passenger_id, book_ref ORDER BY dep_time) AS next_dep_time
    FROM passenger_segments
),

connections AS (
    -- ищем реальные стыковки (прилёт и следующий вылет из того же аэропорта)
    SELECT
        passenger_id,
        book_ref,
        arrival_airport AS hub_airport,
        arr_time,
        next_dep_time,
        EXTRACT(EPOCH FROM (next_dep_time - arr_time)) / 60.0 AS connection_minutes
    FROM ordered_segments
    WHERE next_origin = arrival_airport
      AND next_dep_time > arr_time
      AND (next_dep_time - arr_time) BETWEEN INTERVAL '20 minutes' AND INTERVAL '24 hours'
),

-- список всех аэропортов, через которые пассажиры прилетают/вылетают
all_airport_visits AS (
    SELECT passenger_id, departure_airport AS airport FROM passenger_segments
    UNION ALL
    SELECT passenger_id, arrival_airport AS airport FROM passenger_segments
)

SELECT
    a.airport,
    ad.airport_name ->> 'en' AS airport_name,
    COUNT(DISTINCT a.passenger_id) AS total_passengers,            -- все пассажиры, проходившие через аэропорт
    COUNT(DISTINCT c.passenger_id) AS transit_passengers,          -- пассажиры, имевшие стыковку в этом аэропорту
    ROUND(
        CASE WHEN COUNT(DISTINCT a.passenger_id) = 0 THEN 0
             ELSE COUNT(DISTINCT c.passenger_id)::numeric / COUNT(DISTINCT a.passenger_id)
        END, 4
    ) AS transit_ratio,
    ROUND(AVG(c.connection_minutes), 1) AS avg_connection_min,
    MIN(c.connection_minutes) AS min_connection_min,
    MAX(c.connection_minutes) AS max_connection_min
FROM all_airport_visits a
LEFT JOIN connections c ON c.hub_airport = a.airport
JOIN bookings.airports_data ad ON ad.airport_code = a.airport
GROUP BY a.airport, ad.airport_name
HAVING COUNT(DISTINCT a.passenger_id) > 10   -- фильтр: аэропорты с хотя бы 10 пассажирами
ORDER BY transit_passengers DESC
LIMIT 15;
