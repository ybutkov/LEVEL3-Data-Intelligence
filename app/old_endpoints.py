from enum import Enum

class EndpointKeys(str, Enum):
    AIRPORTS = "airports"
    AIRPORT_BY_CODE = "airport_code"
    COUNTRIES = "countries"
    COUNTRY_BY_CODE = "country_code"
    CITIES = "cities"
    CITY_BY_CODE = "city_code"
    AIRLINES = "airlines"
    AIRLINE_BY_CODE = "airline_code"
    AIRCRAFT = "aircraft"
    AIRCRAFT_BY_CODE = "aircraft_code"
    FLIGHTSTATUS_BY_ROUTE = "flightstatus_by_route"

ENDPOINTS = {
    EndpointKeys.AIRPORTS: "/references/airports",
    EndpointKeys.AIRPORT_BY_CODE: "/references/airports/{airportCode}",
    EndpointKeys.COUNTRIES: "/references/countries",
    EndpointKeys.COUNTRY_BY_CODE: "/references/countries/{countryCode}",
    EndpointKeys.CITIES: "/references/cities",
    EndpointKeys.CITY_BY_CODE: "/references/cities/{cityCode}",
    EndpointKeys.AIRLINES: "/references/airlines",
    EndpointKeys.AIRLINE_BY_CODE: "/references/airlines/{airlineCode}",
    EndpointKeys.AIRCRAFT: "/references/aircraft",
    EndpointKeys.AIRCRAFT_BY_CODE: "/references/aircraft/{aircraftCode}",
    EndpointKeys.FLIGHTSTATUS_BY_ROUTE: "/operations/flightstatus/route/{origin}/{destination}/{date}",
}

LIST_ENDPOINTS = {
    EndpointKeys.AIRPORTS,
    EndpointKeys.COUNTRIES,
    EndpointKeys.CITIES,
    EndpointKeys.AIRLINES,
    EndpointKeys.AIRCRAFT,
}

def get_endpoint(endpoint_key, **params):
    # Check if endpoint exist
    template = ENDPOINTS[endpoint_key]
    endpoint_with_params = template.format(**params)
    full_url = f"{endpoint_with_params}"
    return full_url
