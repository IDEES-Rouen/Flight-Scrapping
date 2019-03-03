
import pymongo
from flightClass import *
from pymongo import MongoClient
import pprint
import pendulum
import subprocess
import psycopg2
import re
import csv

from pathlib import Path

# change password of database ... because i pushed it :(

#conn = psycopg2.connect("dbname=reyman64_flight host=web564.webfaction.com user=reyman64_flight password=eK0EB67ktpoXr58xKtfv")
# conn = psycopg2.connect("dbname=postgres host=localhost user=postgres password=docker")

# Open connection
client = MongoClient()
db = client['flight_project']
CAirport = db['airports']
pyairports = []

# DEF LOAD CSV
dataPath = "/home/reyman/Data/FlightRadar/"

#selectedAirports = ["BIVO","UODD","BIRL","BGBW","CCZ2","BGGH","CEB3","BGSF","CEM3","BGTL","BIAR","CKB6","BIEG","BIHN","BIHU","CNE3","BIIS","BIKF","BIPA","BIRK","CSU2","BISI","BIVM","UUYX","CYAS","CYBB","CYBE","UHMI","UEMA","CYBK","CYCS","EDCP","CYDP","CYCB","CYER","UIKK","UITT","CYFT","CYCO","CYGO","CYGT","CYGW","CYCY","CYGX","CYZS","CYGZ","CYDA","CYHA","UUBW","CYDB","UEMM","CYHO","CYDL","UUDL","CYIK","CYIV","CYDQ","CYKO","CYEK","CYLA","CYLC","CYEU","CYEV","ENJA","CYLU","CYFB","CYMU","CYFO","CYFR","CYNE","CYFS","CYNL","PACK","CYOH","CYPH","CYPO","UWKE","CYQD","CYHI","CYRA","CYHK","CYSF","CYSK","CYHY","CYST","CYTL","CYIO","CYXN","CYZG","CZAC","CZBD","CYKL","CZFD","CZGI","CYLJ","UWLL","EDUW","CZMT","CYMA","CZPB","CYMM","CZSW","UEMT","CZTM","CZWL","PABA","CYOC","PALU","CYOD","PPIZ","CYOJ","EDAH","EDXB","EDXH","PABT","CYPE","EFSI","PACL","PAIM","EGEC","PFYU","EGED","PASV","UNSS","EGEF","PAFR","EGEN","PATL","CYPR","EGEP","PACZ","CYPY","EGER","EGES","PASN","EGET","PAEH","EGEW","PAPB","PAIL","CYQH","PAPM","ULPW","PABM","EGPR","EIDL","UIKB","EKSN","EYVP","ENLK","CYQU","ENNM","ENRS","PAMR","ENSD","ENSG","ENSH","ENSR","CYRB","ENVR","ESNZ","CYRT","ESOH","ESOK","ESST","ESTA","CYSM","ESUD","CYSR","ESUT","EVVA","CYSY","CYTE","CYTH","CYUB","PADL","CYUT","CYUX","PAEM","PAUN","CYVC","PAKU","PAHX","CYVM","PAQT","PAEE","CYVP","PFKA","CYVQ","PFKW","UERL","PAGG","CYVT","PADM","PARS","PAJZ","ULOL","PAMB","UNNE","PABI","PACI","UNKI","PAEG","PANT","CYWY","PAHU","PAHL","PANU","EKEL","PAVE","PAWB","CYXJ","PACE","PAGH","CYXP","UESK","CYXS","CYXT","CYXY","PAJN","CYYD","CYYE","PAGN","PAEL","CYYH","PANR","PAOH","CYYL","PAFE","PAMM","CYYQ","PAHY","PAII","UIAR","PAPE","EKHG","PAPN","PFWS","PAKH","PAKY","PALB","PAFM","CYZF","PABL","CYZH","PAIK","PAOB","PFNO","PASK","CYZU","PFKT","PFEL","CYZW","PAGL","PATE","CZFA","PAIW","CZFM","PAWM","PAKK","PAMK","PFSH","PATC","PACY","PAUK","PAKI","PADQ","PAKF","PAOU","PAAL","PAKW","PAQH","PFKO","PFKU","PACM","PANO","PADY","PAFS","PFAK","PAWI","EDHN","PFCB","PFTO","PACR","PASL","PAHV","PAQC","PAMH","PAML","CNH2","CCD4","CYKG","ENRK","CYBQ","CZWH","CZSN","CYBT","PAOR","ESSZ","PAAQ","CYCR","CYRS","PABR","CYOP","UHMO","CYBF","UHMS","UESU","UHMH","CYJM","UEMU","UEEA","UEMO","UERT","CFJ2","EDHP","BINF","PABE","PACV","PAOM","PASC","UEBB","PAEN","PANA","PAPK","PATJ","EDBH","PAEI","EDCA","PAHO","UIKE","UENS","EDHK","EDHL","EEKU","EERU","BIBD","BIGJ","BIKR","BISF","PAED","PAOT","EGNU","ULWW","UHMW","UERO","UERS","ULAL","UIUN","UNIW","UERA","USSK","UNIB","EGEY","EDXF","EDXR","EDXW","EEEI","EEKA","EEKE","EEPU","ENKA","EETN","EETU","EFET","EFEU","EFHA","EFHF","EFHK","EFHM","EFHN","EFHV","EFIK","EFIM","EFIT","EFIV","EFJO","EFJY","EFKA","EFKE","EFKI","EFKJ","EFKK","EFKM","EFKS","EFKT","EFKU","EFLA","EFLP","EFMA","UIUB","EFME","EFMI","EFNU","EFOU","UUBL","PAGA","EFPI","EFPO","EFPU","EFPY","EFRH","PAGS","EFRN","PAGY","EFRO","PAHC","EFRY","PAHN","EFSA","PALG","EFSE","PAMC","PANC","EFSO","PAMO","EFTP","PANI","EFTS","PAVA","EFTU","PAWG","EFUT","EFVA","EFVR","EFYL","UHML","EGAA","EGAB","EGAC","EFKY","EGAE","PAKN","EGAD","UUBD","BGAM","PAKT","EKSY","EKSR","EKFA","EGNC","PAFA","EPKO","EGNL","EGNM","PAWR","PASX","PAGK","EGNS","EGNT","CYNR","EGNV","ULAE","EGPA","ULAH","EGPB","EGPC","EGPD","EGPE","EGPF","EGPH","EGPI","PACD","EGPK","AK59","EGPL","PASI","EECL","EGPM","EFHE","EGPN","EGPO","EGPU","PADU","EGQL","EGQS","PAVD","UNKM","UACP","UELL","UERP","ESSE","UESO","PAFB","UESS","UEST","EGXD","UHMA","EGXE","UHOO","EGXG","EPOK","EPMB","EGXU","EGXZ","ULAS","ULBC","ULDD","ULKK","ULPB","UMOO","UNII","UNWW","UOHH","UOII","USDD","USHH","EIKN","USHN","USHS","USII","UUMU","USKK","EISG","USMM","USNR","EKAH","USRN","EKBI","USUU","EKCH","EKEB","ENHA","EKGH","ENRI","EKKA","EKLS","EKMB","EKOD","EKPB","EKRK","UUBK","EKRN","EKSB","EKSP","EKSV","UUYW","EKTS","UWKB","EKVD","UWKJ","EKVG","UWKS","ESNF","EKVH","UWLW","EKVJ","EKYT","EVLA","UWPS","EVRA","ENAL","EYSA","ENAN","EYSB","ENAT","EYKA","ENBM","EYKS","ENBN","EYPA","ENBO","EYVI","ENBR","UUYK","EYPP","ENBS","USPT","ENCN","ENDI","ENDU","USDS","ENFG","ENFL","ENGM","ENHD","ENHK","UNKS","ENKB","ENKJ","ENKR","ENLI","ENML","ENMS","ENNA","ENNO","ENOL","ENRO","ENRY","ENSN","EKAE","ENSO","ENST","ENTC","ENVA","ENZV","EPGD","CYZY","BGJN","BGCH","BGAA","PATK","EPSK","EDHF","CYCQ","CYDM","ESCF","ESCK","ESCM","CYHT","ESDF","ESFR","CYJF","ESGG","CYKJ","ESGJ","CYLR","ESGK","PAIN","ESGL","PALH","ESGP","PAPR","CYNN","ESGR","ESGT","ESIA","ESIB","ESKB","ESKK","ESKM","ESKN","ESKV","ESMA","CYXQ","UUDD","ESMG","ESMK","CZEE","ESML","CZFG","ESMO","ESMP","CZJG","ESMQ","CZLQ","ESMS","CZMN","ESMT","ESMV","EDHM","ESMX","CZST","ESNA","ESNC","ESND","UEEE","ESNG","UERR","ESNH","EGOM","ESNJ","EDHB","EGQK","ESNK","EKTD","UHMD","ESNL","EPCE","UHMM","ESNM","UHMP","ESNN","ESNO","ESNP","ESNQ","ESNR","ESKX","UIBB","ESNS","ESTL","ESNT","EVDA","ESNU","EDXM","EVKA","ESNV","EVTA","ESNX","EYKD","ESOE","ESOW","ESPA","ESPE","EDXO","ESQO","ULLI","ESSA","ULMM","ESSB","ESSD","UMII","ESSF","UMKK","ESSK","UMMM","ESSL","UMMS","ESSP","ESSU","UNNT","ESSV","EKSS","UNEE","ESUK","UNOO","USCC","USNN","CYOA","USPP","USRR","USSS","ETNH","EKMS","ETNL","UUEE","ETNS","UUEM","UUWW","UUYY","UWKD","USTR","UWUU","PAGT","PAOO","BGSC","CJX7","PABV","PAPG","PANN","PAWS","UOTT","UNIP","UENK","UENW","UESG","UNLL","UHMF","CYKD","UHMG","CYWJ","UHMK","CZFN","UHMT","CYGH","UHPL","CYPC","UIBV","EDCG","USHB","ENSK","ULAK","ULLP","ULLS","ULNR","ULPP","UNIS","UNKO","UNOS","EGPW","UODN","UOIG","UOOW","UWGG","UROD","UUBA","EGEG","UUBM","EGEO","UUMT","UUWE","UUYI","UUYV","UWKG","UWKV","UWUM","UNIT","USHY","UNTT","XLLN","XLLL","XLMV","XLWF","XUBS","PAKP","PANV","PATQ","PAGM","PAHP","PAKV","PASM","PAVL","PAMY","PARY","CBW4","PASH","UUBC","BIBA","PASA","PAWN","ENHF","ENHV","CYPX","ENMH","CYTQ","ENVD","PARC","UIBS","ULMK","ESUP","PASD","ENNK","ENBV","UOOO","PADE","UUBB","ULAA","USMU","USRO","UUYH","UUYS","PAIG","UUYP","PANW","ULAM","PAVC","ULOO","PAPH","USRK","PATG","UNKL","UNCC","USHU","UUBI","BGUM","PAYA","EPPR","UMMB","UUMO","BGAP","BGCO","EGPT","BGFH","BGGN","PFAL","BGJH","BGMQ","ESNY","BGNN","BGNS","BGQQ","PAWD","BGSS","BGUK","BGUQ","EKKL","UHPK","BIGR","BITN","UHPO"]
selectedAirports = None # ["LFBD"]

def get_files(search_path):
    return [f for f in Path(dataPath).glob('**/*.gz') if f.is_file()]

def buildregex(airport):
    return re.compile(r'/{a}'.format(a=airport), re.I)

def getFlights(airport, code_airport ):

    flights = []

    for pages in airport['pages']:

        # Fenetre glissante
        timestamp_error = False

        try:
            t_starting_capture = pendulum.from_timestamp(pages['departuresPFlight']['timestamp'])
        except:
            print("ERROR OF TIMESTAMP")
            timestamp_error = True

        if timestamp_error != True:

            t_twentyhours_ending_capture = t_starting_capture.add(hours=24)

            # print("start_capture = ", t_starting_capture.format('YYYY-MM-DD HH:mm:ss'))
            # print("end_capture = ", t_twentyhours_ending_capture.format('YYYY-MM-DD HH:mm:ss'))

            data = pages['departuresPFlight']['data']
            for d in data:
                try:
                    airline_icao = d['flight']['airline']['code']['icao']
                    code_line = d['flight']['identification']['number']['default']
                    code_dest_airport = d['flight']['airport']['destination']['code']['icao']
                    timestamp_raw = d['flight']['time']['scheduled']['departure']
                    date_ut = pendulum.from_timestamp(timestamp_raw)
                    if date_ut < t_twentyhours_ending_capture:
                        #print("Add flight > ", code_line, " / ", airline_icao, " / ", code_airport, " / ", date_ut," / ", code_dest_airport)
                        flights.append(Flights(code_line, airline_icao, code_airport, date_ut, code_dest_airport))
                except:
                    print("airline ICA O is None for " , d['flight'])

        # for f in flights:
        #     print(f.ICAO, "/", f.scheduled_time.format('YYYY-MM-DD HH:mm:ss'))
    return flights

def getAirports(CAirport, selectedAirports = None):
    airport_list = []

    if selectedAirports is not None:
        reg = list(map(lambda x : buildregex(x), selectedAirports))
        #curs = CAirport.find({"airport.code_total" : { "$in" : reg}})
        curs = CAirport.find({"$and" : [
            {"airport.code_total" : { "$in" : reg}},
            {"airport.pages.1": { "$exists": True}}]})

    else:
        curs = CAirport.find({"airport.pages.1": { "$exists": True}})

    print(curs.count())
    # db.getCollection('airports').find({"airport.pages.1": { $exists: true}})

    for c in curs:
        airport = c['airport']
        airport_little = airport['code_total'].strip('()').split("/")[1]

        flights = getFlights(airport, airport_little)
        toCSV(flights,airport_little)


def toCSV(flights, airport_icao):

    csvPath = Path.joinpath(Path("csv" ,Path("{a}.csv".format(a=airport_icao))))
    fieldnames = ["airline_icao", "dest_airport_icao", "source_airport_icao", "scheduled_time", "codeline"]

    if csvPath.exists():
        with open(csvPath,'a', newline='') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames)
            for f in flights:
                writer.writerow(f.__dict__)
    else:
        with open(csvPath,'w', newline='') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames)
            writer.writeheader()
            for f in flights:
                writer.writerow(f.__dict__)


def toSQL(flights, conn, airportsFilters=None):
    for f in flights:
        with conn:
            with conn.cursor() as curs:
                curs.execute("""SELECT ai_icao from airlines WHERE ai_icao = %s """,
                             (f.airline_icao,))
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

                    #print("Insert flight with code ", f.source_airport_icao)
                    curs.execute("""INSERT INTO flights (f_ai_icao, f_dest_a_icao, f_src_a_icao, f_sf_id, f_codeline, f_geom)
                    VALUES (%s, %s, %s, %s, %s, ST_MakeLine(%s,%s))""",  # ON CONFLICT DO NOTHING
                                 (f.airline_icao, f.dest_airport_icao, f.source_airport_icao, 0, f.codeline, geom_src,
                                  geom_dest))
                else:
                    print("airline_icao ", f.airline_icao, "not exist")
                    print("airport_icao ", f.dest_airport_icao, "not exist")


for f in get_files(dataPath):

    # Need mongodb-tools ubuntu package
    print("RESTORE PROCESS FOR FILE {file}".format(file=f))

    try:
        subprocess.run("mongorestore  --gzip --noIndexRestore --archive={filename} -vvvvvv".format(filename=f), shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(e.output)

    print("READ DATA")

    getAirports(CAirport, selectedAirports)

    print("DROP PROCESS FOR FILE {file}".format(file=f))
    # Need mongodb-clients ubuntu package
    subprocess.run("mongo --host localhost:27017 flight_project --eval 'db.airports.drop();'", shell=True, check=True)



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