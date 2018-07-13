
from flightClass import *
import psycopg2
import pendulum

#TODO
# - Ajout géométrie Flight
# - mail récap avec info database et metadata
# - gestion Codeshare (moins urgent)
# - gestion des captures dans le temps ?

conn = psycopg2.connect("dbname=reyman64_flight host=web564.webfaction.com user=reyman64_flight password=eK0EB67ktpoXr58xKtfv")

# Delete all
# with conn:
#     with conn.cursor() as curs:
#         curs.execute("""delete FROM flights; """)
#         curs.execute("""delete FROM airports; """)
#         curs.execute("""delete FROM airlines; """)
#         curs.execute("""delete FROM countries; """)

def getFipsCode(name):
    with conn:
        with conn.cursor() as curs:
             curs.execute("""SELECT c_fipscode FROM countries WHERE c_name ILIKE %s """, ['%' + name + '%'])
             return curs.fetchone()[0] if curs.rowcount != 0 else ""

airports = []
countries = []
airlines = []

## READ

# Read and import .dat airlines
airports_filename = "./resources/airports.dat"
countries_filename = "./resources/countries.dat"
airlines_filename = "./resources/airlines.dat"

# with open(airports_filename , 'r') as f:
#     for line in f :
#         item  = line.replace('"', '').split(",")
#         airport = Airport(name = item[1],
#                           city = item[2],
#                           country = item[3],
#                           IATA = None if item[4] == "\\N" else item[4],
#                           ICAO = item[5],
#                           lat = item[6],
#                           lon = item[7],
#                           dst = item[8],
#                           tz = item[9])
#         airports.append(airport)

# with open(countries_filename, 'r') as f:
#     for line in f:
#         try:
#             item = line.replace('"', '').split(",")
#             if (item[2] != "\\N") and (item[1] != "\\N") :
#                 country = Countries(name= item[0], fipscode= item[1], iso3166code= item[2])
#                 countries.append(country)
#             else:
#                 print ("error with : ",item[0] , " country : " ,  item[2] , " / ", item[1])
#         except:
#             print("")

with open(airlines_filename , 'r') as f:
    for line in f :
        item  = line.replace('"', '').split(",")
        airline = Airlines2012(name= item[1],
                               alias = item[2],
                               IATA= None if (item[3] == "\\N" or item[3] == "")  else item[3],
                               ICAO = item[4],
                               callsign=item[5],
                               country=item[6],
                               active=item[7])
        airlines.append(airline)

## INSERT INTO

# COUNTRY
# for country in countries:
#     if (len(country.fipscode) == 2 and len(country.iso3166code) == 2):
#         with conn:
#             with conn.cursor() as curs:
#                 curs.execute("""INSERT INTO countries (c_name, c_fipscode, c_isocode)
#                 VALUES (%s, %s, %s)""",
#                 (country.name, country.fipscode, country.iso3166code))
#     else:
#         print(country.name)

# AIRPORTS
# for airport in airports:
#     fipscode = getFipsCode(airport.country)
#     if (len(airport.ICAO) == 4 and fipscode != "" ):
#         with conn:
#             with conn.cursor() as curs:
#                 curs.execute("""INSERT INTO airports (a_iata, a_icao, a_geom, a_capturedate, a_name, a_country)
#                 VALUES (%s, %s, ST_MakePoint(%s,%s), %s, %s, %s)""",
#                 (airport.IATA, airport.ICAO, airport.lon, airport.lat, airport.time, airport.name, fipscode ))
#     else:
#         print("Country none ! ", airport.country, " / ", airport.IATA," /", airport.ICAO)

# AIRLINES
print("airline size = ", len(airlines))
airlines = set(airlines)
print("airline size = ", len(airlines))

for airline in airlines:
    fipscode = getFipsCode(airline.country)
    if (len(airline.ICAO) == 3 and fipscode != "" ):
        with conn:
            with conn.cursor() as curs:
                curs.execute("""INSERT INTO airlines (ai_icao, ai_iata, ai_name, ai_country)
                VALUES (%s, %s, %s, %s)""", #ON CONFLICT DO NOTHING
                (airline.ICAO, airline.IATA, airline.name, fipscode))
    else:
        print("Airline none ! ", airline.country, " / ", airline.IATA," /", airline.ICAO)
