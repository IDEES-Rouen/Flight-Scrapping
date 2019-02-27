
import psycopg2
import pendulum

#TODO
# - Ajout géométrie Flight
# - mail récap avec info database et metadata
# - gestion Codeshare (moins urgent)
# - gestion des captures dans le temps ?

#conn = psycopg2.connect("dbname=reyman64_flight host=web564.webfaction.com user=reyman64_flight password=eK0EB67ktpoXr58xKtfv")
conn = psycopg2.connect("dbname=postgres host=localhost user=postgres password=docker")

# Delete all
# with conn:
#     with conn.cursor() as curs:
#         curs.execute("""delete FROM flights; """)
#         curs.execute("""delete FROM airports; """)
#         curs.execute("""delete FROM airlines; """)
#         curs.execute("""delete FROM countries; """)


airports = []
countries = []
airlines = []

## READ

# Read and import .dat airlines
airports_filename = "./resources/airports.dat"
countries_filename = "./resources/countries.dat"
airlines_filename = "./resources/airlines.dat"



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
