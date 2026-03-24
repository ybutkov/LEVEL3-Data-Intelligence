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


FLIGHT_STATUS_BY_ROUTE_RULES = {
    "op_fact_flight_status": {
        "airline_id": [NOT_NULL, NOT_EMPTY],
        "flight_number": [NOT_NULL, NOT_EMPTY],
        "dep_airport": [NOT_NULL, NOT_EMPTY],
        "arr_airport": [NOT_NULL, NOT_EMPTY],
    }
}

OPERATIONAL_RULES = {
    "flightstatus_by_route_raw": FLIGHT_STATUS_BY_ROUTE_RULES,

}