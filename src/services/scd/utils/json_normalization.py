import json
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def normalize_names_field(raw_json_str):
    """
    Normalize JSON to ensure Names.Name is always an array.
    Wraps single object in array: {"Name": {...}} -> {"Name": [{...}]}
    """
    try:
        data = json.loads(raw_json_str)

        countries = data.get('CountryResource', {}).get('Countries', {}).get('Country', [])
        for country in countries:
            names_obj = country.get('Names', {}).get('Name')
            if isinstance(names_obj, dict):
                country['Names']['Name'] = [names_obj]
        
        cities = data.get('CityResource', {}).get('Cities', {}).get('City', [])
        for city in cities:
            names_obj = city.get('Names', {}).get('Name')
            if isinstance(names_obj, dict):
                city['Names']['Name'] = [names_obj]
        
        airlines = data.get('AirlineResource', {}).get('Airlines', {}).get('Airline', [])
        for airline in airlines:
            names_obj = airline.get('Names', {}).get('Name')
            if isinstance(names_obj, dict):
                airline['Names']['Name'] = [names_obj]
        
        airports = data.get('AirportResource', {}).get('Airports', {}).get('Airport', [])
        for airport in airports:
            names_obj = airport.get('Names', {}).get('Name')
            if isinstance(names_obj, dict):
                airport['Names']['Name'] = [names_obj]
        
        return json.dumps(data)
    except:
        return raw_json_str
