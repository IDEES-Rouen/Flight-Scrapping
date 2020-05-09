from pathlib import Path
from pymongo import MongoClient
import subprocess
from flightClass import *

class MongoDBUtils():
    def __init__(self, datapath):
        self.client = MongoClient()
        self.db = self.client['flight_project']
        self.CAirport = self.db['airports']
        self.pyairports = []
        self.dataPath = datapath

    def import_data(self,f):
        # Need mongodb-tools ubuntu package
        print("RESTORE PROCESS FOR FILE {file}".format(file=f))

        try:
            subprocess.run("mongorestore  --gzip --noIndexRestore --archive={filename} -vvvvvv".format(filename=f),
                           shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(e.output)

    def drop_data(self,f):

        print("DROP PROCESS FOR FILE {file}".format(file=f))
        # Need mongodb-clients ubuntu package
        try:
            subprocess.run("mongo --host localhost:27017 flight_project --eval 'db.airports.drop();'", shell=True,
                           check=True)

        except subprocess.CalledProcessError as e:
            print(e.output)

    def loadDataFromMongoDB(self,selectedFiles = None, selectedAirports = None):
        flights_by_airport = []

        if selectedFiles!= None:
            testedFiles = []
            # Get files corresponding to files
            for f in selectedFiles:
                file = self.dataPath.joinpath(f)
                if file.exists() :
                    testedFiles.append(file)

            for f in testedFiles:
                self.import_data(f)
                flights_by_airport.append(getAirports(self.CAirport, selectedAirports))
                self.drop_data(f)

        else:
            for f in get_files(self.dataPath):
                flights_by_airport.append(getAirports(self.CAirport, selectedAirports))


        return flights_by_airport

    def get_files(self,search_path):
        return [f for f in Path(search_path).glob('**/*.gz') if f.is_file()]


def getFlights(airport, code_airport ):

    flights = []

    # EXTRACT ALL AIRPORTS :
    # - IATA
    # - ICAO
    # - FUSEAU HORAIRE
    # - LAT LON
    #print(airport['name'])

    for pages in airport['pages']:
        departures_error = False

        try:
            departures = pages['departuresPFlight']
            #t_starting_capture = pendulum.from_timestamp(pages['departuresPFlight']['timestamp'])
        except:
            #print("NO DEPARTURES")
            departures_error = True

        if departures_error != True:

            #t_twentyhours_ending_capture = t_starting_capture.add(hours=24)

            #print("start_capture = ", t_starting_capture.format('YYYY-MM-DD HH:mm:ss'))
            #print("end_capture = ", t_twentyhours_ending_capture.format('YYYY-MM-DD HH:mm:ss'))

            # TODO : export aussi des [arrivals][data]
            data = pages['departuresPFlight']['data']
            for d in data:
                try:
                    airline_icao = d['flight']['airline']['code']['icao']
                except:
                    airline_icao = ""

                try:
                    airline_status = d['flight']['status']['text']
                except:
                    airline_status = ""

                try:
                    airline_iata = d['flight']['airline']['code']['iata']
                except:
                    airline_iata = ""

                try:
                    code_line = d['flight']['identification']['number']['default']
                except:
                    code_line = ""
                try:
                    aircraft_type = d['flight']['aircraft']['model']['code']
                except:
                    aircraft_type = ""
                try:
                    aircraft_registration = d['flight']['aircraft']['registration']
                except:
                    aircraft_registration = ""
                try:
                    code_dest_airport = d['flight']['airport']['destination']['code']['icao']
                except:
                    code_dest_airport = ""
                try :
                    timestamp_raw_departures = d['flight']['time']['scheduled']['departure']
                    date_ut = pendulum.from_timestamp(timestamp_raw_departures)

                except:
                    timestamp_raw_departures = ""
                    date_ut = ""
                try:
                    timestamp_raw_arrivals = d['flight']['time']['scheduled']['arrival']
                    date_ut_arrivals = pendulum.from_timestamp(timestamp_raw_arrivals)
                except:
                    timestamp_raw_arrivals = ""
                    date_ut_arrivals = ""

                flights.append(Flights(code_line, airline_status, airline_icao, airline_iata, aircraft_type, aircraft_registration, code_airport, code_dest_airport, date_ut, date_ut_arrivals))

        # for f in flights:
        #     print(f.ICAO, "/", f.scheduled_time.format('YYYY-MM-DD HH:mm:ss'))
    return flights

def getAirports(CAirport, selectedAirports = None):
    airport_list = []
    flights_list_by_airports = []

    if selectedAirports is not None:
        reg = list(map(lambda x : buildregex(x), selectedAirports))
        #curs = CAirport.find({"airport.code_total" : { "$in" : reg}})
        curs = CAirport.find({"$and" : [
           {"airport.code_total" : { "$in" : reg}},
           {"airport.pages.1": { "$exists": True}}]})

    else:
        curs = CAirport.find({"airport.pages.0": { "$exists": True}})

    #print(curs.count())
    # db.getCollection('airports').find({"airport.pages.1": { $exists: true}})

    for c in curs:
        airport = c['airport']
        airport_little = airport['code_total'].strip('()').split("/")[1]

        flights = getFlights(airport, airport_little)
        flights_list_by_airports.append(flights)
        #toCSV(flights,airport_little)

    return flights_list_by_airports