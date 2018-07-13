
import pymongo
from flightClass import *
from pymongo import MongoClient
import pprint
import pendulum
import psycopg2

conn = psycopg2.connect("dbname=reyman64_flight host=web564.webfaction.com user=reyman64_flight password=eK0EB67ktpoXr58xKtfv")

# Open connection
client = MongoClient()
db = client['flight_project']
airports = db['airports']
pyairports = []

for post in airports.find():
    #pprint.pprint(post['airport']['code_little'])
    pyairports.append(post['airport'])
    if post['airport']['code_little'] == "JFK":
        orly = post['airport']

#print(len(pyairports))
#print(pyairports[0]['pages'][0]['departuresPFlight']['data'][0]['flight'])
airport_little = orly['code_total'].strip('()').split("/")[1]
flights = []

for pages in orly['pages']:
    # Fenetre glissante
    t_starting_capture = pendulum.from_timestamp(pages['departuresPFlight']['timestamp'])
    t_twentyhours_ending_capture = t_starting_capture.add(hours=24)
    #
    # print("start_capture = ", t_starting_capture.format('YYYY-MM-DD HH:mm:ss'))
    # print("end_capture = ", t_twentyhours_ending_capture.format('YYYY-MM-DD HH:mm:ss'))

    data = pages['departuresPFlight']['data']
    for d in data:
        airline_icao = d['flight']['airline']['code']['icao']

        if airline_icao != None:
            code_line = d['flight']['identification']['number']['default']
            code_airport = airport_little
            code_dest_airport = d['flight']['airport']['destination']['code']['icao']
            timestamp_raw = d['flight']['time']['scheduled']['departure']
            date_ut = pendulum.from_timestamp(timestamp_raw)
            if date_ut < t_twentyhours_ending_capture:
                flights.append(Flights(code_line, airline_icao, code_airport, date_ut, code_dest_airport))
        else:
            print("airline ICAO is None")
# for f in flights:
#     print(f.ICAO, "/", f.scheduled_time.format('YYYY-MM-DD HH:mm:ss'))

for f in flights:
        with conn:
            with conn.cursor() as curs:
                curs.execute("""SELECT ai_icao from airlines WHERE ai_icao = %s """,
                             (f.airline_icao, ))
                ai_icao = curs.rowcount

                curs.execute("""SELECT a_icao from airports WHERE a_icao = %s """,
                             (f.dest_airport_icao,))
                a_icao = curs.rowcount

                if ai_icao != 0 and a_icao != 0:

                    geom_dest = curs.execute("""SELECT ST_AsText(a_geom) from airports WHERE a_icao = %s """,
                                 (f.dest_airport_icao,))
                    geom_dest = curs.fetchone()[0]

                    geom_src = curs.execute("""SELECT ST_AsText(a_geom) from airports WHERE a_icao = %s """,
                                 (f.source_airport_icao,))
                    geom_src = curs.fetchone()[0]

                    curs.execute("""INSERT INTO flights (f_ai_icao, f_dest_a_icao, f_src_a_icao, f_sf_id, f_codeline, f_geom)
                VALUES (%s, %s, %s, %s, %s, ST_MakeLine(%s,%s))""", #ON CONFLICT DO NOTHING
                (f.airline_icao, f.dest_airport_icao, f.source_airport_icao , 0, f.codeline, geom_src, geom_dest ))
                else:
                    print("airline_icao ", f.airline_icao , "not exist")
                    print("airport_icao ", f.dest_airport_icao , "not exist")

#

# insert from data imported by json, cross validation :
# https://stackoverflow.com/questions/4069718/postgres-insert-if-does-not-exist-already

# INSERT
# INTO
# distributors(did, dname)
# VALUES(5, 'Gizmo Transglobal'), (6, 'Associated Computing, Inc')
# ON
# CONFLICT(did)
# DO
# UPDATE
# SET
# dname = EXCLUDED.dname;