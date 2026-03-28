from pyspark import pipelines as dp
from pyspark.sql.functions import length, explode, from_json


def NOT_NULL(c): return f"{c} IS NOT NULL"
def NOT_EMPTY(c): return f"length(trim({c})) > 0"
def LENGTH(c, n): return f"length({c}) = {n}"
def EQUALS(c, v): return f"({c}) = {v}"
def IS_UPPER(c): return f"{c} = upper({c})"
def ONLY_LETTERS(c): return f"{c} REGEXP '^[A-Z]+$'"
def IS_DOUBLE(c): return f"CAST({c} AS DOUBLE) IS NOT NULL"
def LAT_RANGE(c): return f"CAST({c} AS DOUBLE) BETWEEN -90 AND 90"
def LON_RANGE(c): return f"CAST({c} AS DOUBLE) BETWEEN -180 AND 180"
def AIRLINE_LENGTH(c): return f"length({c}) BETWEEN 2 AND 3"


COUNTRY_RULES = {
    "ref_dim_country": {
        "country_code_not_null": NOT_NULL("country_code"),
        "country_code_len":      LENGTH("country_code", 2),
    },
    "ref_country_names_flat": {
        "country_code_not_null":  NOT_NULL("country_code"),
        "country_code_len":       LENGTH("country_code", 2),
        "lang_code_not_null":     NOT_NULL("language_code"),
        "lang_code_len":          LENGTH("language_code", 2),
        "country_name_not_empty": NOT_EMPTY("country_name")
    }
}

CITY_RULES = {
    "ref_dim_city": {
        "city_code_len":    LENGTH("city_code", 3),
        "country_code_len": LENGTH("country_code", 2),
        "city_code_not_null": NOT_NULL("city_code")
    },
    "ref_city_names_flat": {
        "city_code_len":  LENGTH("city_code", 3),
        "lang_code_len":  LENGTH("language_code", 2),
        "name_not_empty": NOT_EMPTY("city_name")
    },
    "ref_city_airport_map": {
        "city_code_len":    LENGTH("city_code", 3),
        "airport_code_len": LENGTH("airport_code", 3)
    }
}

AIRPORT_RULES = {
    "ref_dim_airport": {
        "airport_code_len": LENGTH("airport_code", 3),
        "city_code_len":    LENGTH("city_code", 3),
        "country_code_len": LENGTH("country_code", 2),
        "location_type":    EQUALS("location_type", "'Airport'"),
        
        "lat_is_double":    IS_DOUBLE("latitude"),
        "lat_range":        LAT_RANGE("latitude"),
        "lon_is_double":    IS_DOUBLE("longitude"),
        "lon_range":        LON_RANGE("longitude")
    },
    "ref_airport_names_flat": {
        "airport_code_len": LENGTH("airport_code", 3),
        "lang_code_len":    LENGTH("language_code", 2),
        "name_not_empty":   NOT_EMPTY("airport_name"),
    }
}

AIRLINE_RULES = {
    "ref_dim_airline": {
        "airline_id_not_null": NOT_NULL("airline_id"),
        "airline_id_len":      LENGTH("airline_id", 3),
        "icao_code_not_null": NOT_NULL("icao_code"),
        "icao_code_len":      AIRLINE_LENGTH("icao_code"),
    },
    "ref_airline_names_flat": {
        "airline_id_not_null":  NOT_NULL("airline_id"),
        "airline_id_len":       LENGTH("airline_id", 3),
        "icao_code_not_null":   NOT_NULL("icao_code"),
        "icao_code_len":        AIRLINE_LENGTH("icao_code"),
        "lang_code_not_null":   NOT_NULL("language_code"),
        "lang_code_len":        LENGTH("language_code", 2),
        "name_not_empty":       NOT_EMPTY("airline_name"),
    }
}

AIRCRAFT_RULES = {
    "ref_dim_aircraft": {
        "aircraft_code_not_null":       NOT_NULL("aircraft_code"),
        "aircraft_code_len":            LENGTH("aircraft_code", 3),
        "airline_equip_code_not_null":  NOT_NULL("airline_equip_code"),
    },
    "ref_aircraft_names_flat": {
        "aircraft_code_len":            LENGTH("aircraft_code", 3),
        "lang_code_len":                LENGTH("language_code", 2),
        "name_not_empty":               NOT_EMPTY("aircraft_name"),
    }
}

OPERATIONAL_RULES = {
    "op_fact_flight_status": {
        "marketing_airline_not_null":   NOT_NULL("flight.MarketingCarrier.AirlineID"),
        "marketing_flight_not_null":    NOT_NULL("flight.MarketingCarrier.FlightNumber"),
        "departure_airport_not_null":   NOT_NULL("flight.Departure.AirportCode"),
        "arrival_airport_not_null":     NOT_NULL("flight.Arrival.AirportCode"),
        "scheduled_departure_not_null": NOT_NULL("flight.Departure.ScheduledTimeUTC.DateTime"),
        "flight_status_not_null":       NOT_NULL("flight.FlightStatus.Code"),
    },
    "op_fact_flight_status_quarantine": {
        "marketing_airline_not_null":   NOT_NULL("flight.MarketingCarrier.AirlineID"),
        "marketing_flight_not_null":    NOT_NULL("flight.MarketingCarrier.FlightNumber"),
        "departure_airport_not_null":   NOT_NULL("flight.Departure.AirportCode"),
        "arrival_airport_not_null":     NOT_NULL("flight.Arrival.AirportCode"),
    }
}

REFERENCE_RULES = {
    "countries_raw": COUNTRY_RULES,
    "cities_raw": CITY_RULES,
    "airports_raw": AIRPORT_RULES,
    "airlines_raw": AIRLINE_RULES,
    "aircraft_raw": AIRCRAFT_RULES,
}

def apply_validations(df, checks):
    # Accept either a dict of name->condition or a list of (name, condition)
    if isinstance(checks, dict):
        items = checks.items()
    else:
        items = checks
    for name, condition in items:
        df = df.expect_or_drop(name, condition)
    return df

