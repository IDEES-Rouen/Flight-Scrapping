from flightClass import *
#from database import DatabaseCursor

def load_countries():
    countries_filename = "./resources/countries.dat"
    #db = DatabaseCursor('resources/seb_local.json')

    countries = []
    with open(countries_filename, 'r') as f:
        for line in f:
            try:
                item = line.replace('"', '').split(",")
                #print("item = ", item)
                country = Countries(name= item[0],
                                    fipscode= None if item[1] == "\\N" else item[1],
                                    iso3166code= None if item[2] == "\\N" else item[2])
                # we consider that one official code is a minimum
                if country.fipscode or country.iso3166code:
                    #print("Append : ", country.name, "with FIPS ", country.fipscode)
                    countries.append(country)
                else:
                    print("Country ", country.name, "don't have iso or fips code")
            except:
                print("FAILED")
    return countries

def load_airport():
    airports_filename = "./resources/airports.dat"

    airports = []
    import re
    with open(airports_filename, 'r') as f:
        for line in f:
            # Clean item !
            item = line.replace("\\N", "\"\"")
            item = list(map(lambda x: x.replace(",","").replace("\"",""), re.findall(r'"[^"\\]*(?:\\.[^"\\]*)*"|\d+\.?\d*',item)))
            airport = Airport(name=item[1],
                              city=item[2],
                              country=item[3],
                              IATA=None if item[4] == "\\N" else item[4],
                              ICAO=item[5],
                              lat=item[6],
                              lon=item[7],
                              dst=item[8],
                              tz=item[9])
            airports.append(airport)

    return airports

def load_airlines():

    airlines_filename = "./resources/airlines.dat"
    airlines = []
    import re

    with open(airlines_filename, 'r') as f:
        for line in f:
            item = line.replace("\\N", "\"\"")
            item = list(map(lambda x: x.replace(",","").replace("\"",""), re.findall(r'"[^"\\]*(?:\\.[^"\\]*)*"|\d+\.?\d*',item)))
            airline = Airlines2012(name=item[1],
                                   alias=item[2],
                                   IATA=None if (item[3] == "\\N" or item[3] == "") else item[3],
                                   ICAO=item[4],
                                   callsign=item[5],
                                   country=item[6],
                                   active=item[7])

            airlines.append(airline)

    return airlines

#
# with db as curs:
#
#     for country in countries:
#         print(country.iso3166code)
#         curs.execute("""INSERT INTO countries (c_name, c_isocode,c_fipscode)
#         VALUES (%s, %s, %s)""", (country.name, country.iso3166code, country.fipscode))
#
#     db.conn.commit()
