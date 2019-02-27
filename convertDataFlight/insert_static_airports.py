from flightClass import *
from database import DatabaseCursor
import uuid

airports_filename = "./resources/airports.dat"
db = DatabaseCursor('resources/seb_local.json')

def getFipsCode(curs, name):
    curs.execute("""SELECT c_isocode FROM countries WHERE c_name ILIKE %s """, ['%' + name + '%'])
    return curs.fetchone()[0] if curs.rowcount != 0 else ""

airports = []

with open(airports_filename , 'r') as f:
    for line in f :
        item  = line.replace('"', '').split(",")
        airport = Airport(name = item[1],
                          city = item[2],
                          country = item[3],
                          IATA = None if item[4] == "\\N" else item[4],
                          ICAO = item[5],
                          lat = item[6],
                          lon = item[7],
                          dst = item[8],
                          tz = item[9])
        airports.append(airport)

with db as curs:

    for airport in airports:
        uuid = uuid.uuid4()
        isocode = getFipsCode(curs, airport.country)
        if (len(airport.ICAO) == 4 and isocode != "" ):
           curs.execute("""INSERT INTO airports (a_iata, a_icao, a_geom, a_capturedate, a_name, a_country)
                           VALUES (%s, %s, ST_MakePoint(%s,%s), %s, %s, %s)""",
                           (airport.IATA, airport.ICAO, airport.lon, airport.lat, airport.time, airport.name, isocode ))
        else:
            print("Country none ! ", airport.country, " / ", airport.IATA," /", airport.ICAO)

    db.conn.commit()
