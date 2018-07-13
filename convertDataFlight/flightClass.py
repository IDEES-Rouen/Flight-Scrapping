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
    def __init__(self,codeline, airline_icao, source_airport_icao, scheduled_time, dest_airport_icao):
        self.codeline = codeline
        self.airline_icao = airline_icao
        self.source_airport_icao = source_airport_icao
        self.dest_airport_icao = dest_airport_icao
        self.scheduled_time = scheduled_time

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