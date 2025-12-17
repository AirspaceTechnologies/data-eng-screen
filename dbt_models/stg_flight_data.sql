{{
    config(
        materialized='view'
    )
}}

-- Transform raw flight data from AviationStack API into clean, usable format
-- Source: stg_flights table loaded from AviationStack API JSON

SELECT
    flight_date::DATE AS flight_date,
    flight_status::VARCHAR AS flight_status,

    -- Extract departure airport information
    departure:airport::VARCHAR AS departure_airport,
    departure:iata::VARCHAR AS departure_iata,

    -- Extract arrival airport information
    arrival:airport::VARCHAR AS arrival_airport,
    arrival:iata::VARCHAR AS arrival_iata,

    -- Extract airline information
    airline:name::VARCHAR AS airline_name,
    airline:iata::VARCHAR AS airline_iata,

    -- Add metadata
    load_timestamp::TIMESTAMP AS loaded_at

FROM {{ source('flight_data', 'stg_flights') }}

WHERE flight_date IS NOT NULL
