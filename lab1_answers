SELECT *
FROM flights
WHERE arrival_airport = 'SVO'
  AND COALESCE(actual_arrival, scheduled_arrival)::date = '2017-07-22'
  AND COALESCE(actual_arrival, scheduled_arrival)::time BETWEEN '16:00' AND '19:00'
ORDER BY COALESCE(actual_arrival, scheduled_arrival) ASC;


UPDATE ticket_flights
SET amount = amount + 1000
WHERE flight_id IN (
    SELECT flight_id
    FROM flights
    WHERE (COALESCE(actual_departure, scheduled_departure)::time BETWEEN '07:00' AND '10:00')
       OR (COALESCE(actual_departure, scheduled_departure)::time BETWEEN '17:00' AND '20:00')
);


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
