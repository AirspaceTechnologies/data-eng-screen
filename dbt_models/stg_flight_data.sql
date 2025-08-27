SELECT
    flight_date::date AS flight_date,
    flight_status::string AS status,
    departure.airport::string AS departure_airport,
    arrival.airport::string AS arrival_airport,
    airline.name::string AS airline_name
FROM {{ source('flight_data', 'stg_flights') }}
