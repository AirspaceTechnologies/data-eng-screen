select
    raw:flight_date::date as flight_date,
    raw:flight_status::string as status,
    raw:departure.airport::string as departure_airport,
    raw:arrival.airport::string as arrival_airport,
    raw:airline.name::string as airline_name
from {{ source('flight_data', 'stg_flights') }}

