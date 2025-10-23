# Lab_1
## Ответы для lab1
### 1.2
В этом запросе использовались SELECT * (Выбрать все столбцы из таблицы), FROM (из какой таблицы), WHERE, AND, COALESCE (Функция, которая берет первый элементт не равный NULL), ORDER BY (Сортировка), ASC (По возрастанию)
``` sql
SELECT *
FROM flights
WHERE arrival_airport = 'SVO'
  AND COALESCE(actual_arrival, scheduled_arrival)::date = '2017-07-22'
  AND COALESCE(actual_arrival, scheduled_arrival)::time BETWEEN '16:00' AND '19:00'
ORDER BY COALESCE(actual_arrival, scheduled_arrival) ASC;
```
### 2.3
В этом запросе использовались UPDATE (Обновить таблицу), SET (Установить-поменять значения), IN(...) (Внутренний запрос. Запросил с фильтрами нужные flight_id)
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
### 3.3
В этом запросе я беру результаты внутреннего запроса и составляю таблицу с самыми северными аэропортами во внешнем. Я использую ROW_NUMBER() OVER(...) для того, чтобы пронумеровать уже отсортированные для каждой временной зоны по убыванию широты аэропорты. Из этого внутреннего запроса составляю таблицу, куда добавляю из каждой зоны аэропорт с номером 1. Использовал PARTITION BY (Разделение по зонам, в данном случае), AS ... ()
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
В этом запросе я:
1.  Cобрал все сегменты полётов пассажиров за июль 2017(У меня не хватило памяти подсчитать всё, ниже прикрепил без этого фильтра) (passenger_segments). (Использовались JOIN для того, чтобы слить все данные в одну таблицу)
2.	Для каждого пассажира упорядочили сегменты по времени и «посмотрели вперёд», какой следующий сегмент(посадка или пересадка) у него будет (ordered). (Использовались LEAD(...) для того чтобы найти следующий по хронологии сегмент)
3.	Отфильтровали пары последовательных сегментов, которые выглядят как пересадка в том же аэропорту и в разумный промежуток времени (connections). (Проверяются условия пересадки с помощью WHERE и AND, т.е. один и тот же аэропорт и интервал от 20 минут до 24 часов)
4.	Посчитали, сколько уникальных пассажиров делали пересадку в каждом аэропорту и среднее время пересадки (transit_stats). (Используем COUNT(DICTINCT) для подсчета уникальных пассажиров)
5.	Посчитали, сколько уникальных пассажиров вообще было связано с каждым аэропортом (вылеты+прилёты) (total_stats). (Используем UNION ALL для объединения всех временных таблиц и DICTINCT для уникальности)
6.	Соединили статистику, добавили название аэропорта, посчитали долю транзитных пассажиров и вывели топ-15 (SELECT ... ORDER BY ... LIMIT).
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
Без фильтра на июль 2017:
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
