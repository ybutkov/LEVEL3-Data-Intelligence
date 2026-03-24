from pyspark.sql.functions import length, trim, upper, regexp_like

NOT_NULL = lambda c: c.isNotNull()
NOT_EMPTY = lambda c: length(trim(c)) > 0
LENGTH_2 = lambda c: length(c) == 2
LENGTH_3 = lambda c: length(c) == 3
LENGTH_4 = lambda c: length(c) == 4
AIRLINE_LENGTH = lambda c: length(c).isin(2, 3)
IS_UPPER = lambda c: c == upper(c)
ONLY_LETTERS = lambda c: regexp_like(c, r"^[A-Z]+$")
IS_DOUBLE = lambda c: c.cast("double").isNotNull()
LAT_RANGE = lambda c: (c.cast("double") >= -90) & (c.cast("double") <= 90)
LON_RANGE = lambda c: (c.cast("double") >= -180) & (c.cast("double") <= 180)


COUNTRY_RULES = {
    "ref_dim_country": {
        "country_code": [NOT_NULL,LENGTH_2]
    },
    "ref_country_names_flat": {
        "country_code": [ NOT_NULL, LENGTH_2 ],
        "language_code": [ NOT_NULL, LENGTH_2 ],
        "country_name": [ NOT_NULL, NOT_EMPTY ],
    },
}


CITY_RULES = {
    "ref_dim_city": {
        "city_code": [ NOT_NULL, LENGTH_3 ],
        "country_code": [ NOT_NULL, LENGTH_2 ],
    },
    "ref_city_names_flat": {
        "city_code": [ NOT_NULL, LENGTH_3],
        "language_code": [ NOT_NULL, LENGTH_2 ],
        "city_name": [ NOT_NULL, NOT_EMPTY ],
    },
    "ref_city_airport_map": {
        "city_code": [ NOT_NULL, LENGTH_3 ],
        "airport_code": [ NOT_NULL, LENGTH_3 ],
    },
}


AIRPORT_RULES = {
    "ref_dim_airport": {
        "airport_code": [ NOT_NULL, LENGTH_3 ],
        "city_code": [ NOT_NULL,  LENGTH_3 ],
        "country_code": [ NOT_NULL,  LENGTH_2 ],
        # TODO: Do we need to validate lat/lon values?
        "latitude": [ NOT_NULL, IS_DOUBLE, LAT_RANGE ],
        "longitude": [ NOT_NULL, IS_DOUBLE, LON_RANGE ],
    },
    "ref_airport_names_flat": {
        "airport_code": [ NOT_NULL, LENGTH_3 ],
        "language_code": [ NOT_NULL,  LENGTH_2 ],
        "airport_name": [ NOT_NULL, NOT_EMPTY ],
    },
}


AIRLINE_RULES = {
    "ref_dim_airline": {
         # TODO: AIRLINE_LENGTH 2 or 3 ???
        "airline_id": [ NOT_NULL, LENGTH_2 ],
    },
    "ref_airline_names_flat": {
        "airline_id": [ NOT_NULL, LENGTH_2 ],
        "language_code": [ NOT_NULL, LENGTH_2 ],
        "airline_name": [ NOT_NULL, NOT_EMPTY ],
    },
}


AIRCRAFT_RULES = {
    "ref_dim_aircraft": {
        "aircraft_code": [ NOT_NULL, LENGTH_3],
    },
    "ref_aircraft_names_flat": {
        "aircraft_code": [ NOT_NULL, LENGTH_3 ],
        "language_code": [ NOT_NULL, LENGTH_2 ],
        "aircraft_name": [ NOT_NULL, NOT_EMPTY ],
    },
}


REFERENCE_RULES = {
    "countries_raw": COUNTRY_RULES,
    "cities_raw": CITY_RULES,
    "airports_raw": AIRPORT_RULES,
    "airlines_raw": AIRLINE_RULES,
    "aircraft_raw": AIRCRAFT_RULES,
}

