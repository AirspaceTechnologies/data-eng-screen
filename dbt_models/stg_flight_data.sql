select
    flight_date::date as flight_date,
    flight_status::string as status,
    departure.airport::string as departure_airport,
    arrival.airport::string as arrival_airport,
    airline.name::string as airline_name
from {{ source('flight_data', 'stg_flights') }}