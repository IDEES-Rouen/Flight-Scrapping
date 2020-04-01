import pendulum

class Airlines2012():
    def __init__(self, name, alias, IATA, ICAO, callsign, country, active):
        self.name = name
        self.alias = alias
        self.IATA = IATA
        self.ICAO = ICAO
        self.callsign = callsign
        self.country = country
        self.active = active

    def __eq__(self, other):
        return self.ICAO== other.ICAO

    def __hash__(self):
        return hash(self.ICAO)


class Flights():
    def __init__(self,codeline, airline_status, airline_icao, airline_iata, aircraft_type, aircraft_registration, code_airport, code_dest_airport, date_ut,date_ut_arrivals):
        self.idflight_icao = codeline
        self.airline_icao = airline_icao
        self.airline_status = airline_status
        self.airline_iata = airline_iata
        self.aircraft_type = aircraft_type
        self.aircraft_registration = aircraft_registration
        self.source_airport_icao = code_airport
        self.dest_airport_icao = code_dest_airport
        self.time_dep = date_ut
        self.time_arrival = date_ut_arrivals

class Countries():
    def __init__(self,name, fipscode, iso3166code):
        self.name = name
        self.fipscode = fipscode
        self.iso3166code = iso3166code

class Airport():
    def __init__(self,name, city, country, IATA, ICAO, lat, lon, dst, tz):
        self.name = name
        self.city = city
        self.country = country
        self.IATA = IATA
        self.ICAO = ICAO
        self.lat = lat
        self.lon = lon
        self.time = pendulum.today()
        self.dst = dst
        self.tz = tz