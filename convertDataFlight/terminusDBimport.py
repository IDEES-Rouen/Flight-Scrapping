
import woqlclient.woqlClient as woql
from woqlclient import WOQLQuery
import woqlclient.errors as woqlError

import woqlclient.woqlDataframe as wdf
import woqlDiagnosis as wary
import pandas as pd

#from readMongoDBFiles import *

def create_schema(client):
    #
    # We first create an abstract class to represent ephemeral entities - things that have lifespans
    #
    schema = WOQLQuery().when(True).woql_and(
        WOQLQuery().doctype("EphemeralEntity").label("Ephemeral Entity").description("An entity that has a lifespan").abstract()
        .property("lifespan_start", "dateTime").label("Existed From")
        .property("lifespan_end", "dateTime").label("Existed To"),
    #
    # This allows us to attach existence start and end times to entities, in a consistent way.
    # Then we create our four actual concrete classes, each as a subclass of this one:
    #
    # That gives you the meta-structure - the major relationships between airports, airlines, countries and flights -
    # you can then add whatever other properties you want. All the entities also have lifespan_start and lifespan_end
    # properties inherited from EphemeralEntity

        WOQLQuery().add_class("Country").label("Country").description("A nation state").parent("EphemeralEntity")
            .property("country_id", "string").label("Id")
            .property("country_name", "string").label("Name")
            .property("country_iso_code", "string" ).label("ISO Code")
            .property("country_fips_code", "string").label("FIPS Code"), #primary key

        WOQLQuery().add_class("Airline").label("Airline").description("An operator of airplane flights").parent("EphemeralEntity")
            .property("icao_code", "string").label("ICAO Code")  # primary key
            .property("registered_in", "Country").label("Registered In"),

        WOQLQuery().add_class("Airport").label("Airport").description("An airport where flights terminate").parent("EphemeralEntity")
            .property("situated_in", "Country").label("Situated In")
            .property("icao_code", "string").label("ICAO Code") # primary key
            .property("iata_code", "string").label("IATA Code")
            .property("name", "string").label("Name"),

        WOQLQuery().add_class("Flight").label("Flight").description("A flight between airports").parent("EphemeralEntity")
            .property("departs", "Airport").label("Departs")
            .property("arrives", "Airport").label("Arrives")
            .property("operated_by", "Airline").label("Operated By"))

    try:
        print("[Building schema..]")
        with wary.suppress_Terminus_diagnostics():
            schema.execute(client)
    except Exception as e:
        wary.diagnose(e)

def country_query(country):

    if country.fipscode:
        country_code = "_fips_" + country.fipscode
    else:
        country_code = "_iso_" + country.iso3166code

    matches_c = WOQLQuery().woql_and(
            WOQLQuery().idgen("doc:Country", [country_code], "v:country_id" + country_code))

    insert_c = WOQLQuery().insert("v:country_id" + country_code, "Country"). \
        property("country_id", {'@type': 'xsd:string', '@value': "doc:Country" + country_code}). \
        property("country_name", {'@type': 'xsd:string', '@value': country.name}).\
        property("country_iso_code", {'@type': 'xsd:string', '@value': country.iso3166code}).\
        property("country_fips_code", {'@type': 'xsd:string', '@value': country.fipscode})

    return [matches_c, insert_c]

def is_empty(q):
    '''
        Test for an empty query result
        :param q:   Woql query result
    '''
    return len(q['bindings']) == 0

def airport_query(airport):

    print("country_id = ", airport.country_id)
    print("country_code = ", country_code)
    print("airport_ICAO = ", airport.ICAO)

    matches_c = WOQLQuery().woql_and(WOQLQuery().idgen("doc:Airport", [airport.ICAO], "v:Airport_id" + airport.ICAO))

    insert_c = WOQLQuery().insert("v:Airport_id" + airport.ICAO, "Airport").\
        property("situated_in", airport.country_id)

    return [matches_c, insert_c]

# TODO Rewrite as a recursive function, taking elements n by n
def generate_bulk_insert(elements, fnQueries):
    i = 1
    matches_query = []
    insert_query = []
    nb = len(elements)

    for e in elements:

        nb -= 1
        if (nb == 0) :
            yield [matches_query,insert_query]
        elif (i % 10 == 0):

            match, insert = fnQueries(e)

            # exist and/or possible
            if match and insert:
                # one for array and one for insert
                matches_query.append(match)
                insert_query.append(insert)
            else:
                print("not possible for ", e)

            yield [matches_query, insert_query]
            matches_query = []
            insert_query = []
            i = 1
        else:
            match, insert = fnQueries(e)

            # exist and/or possible
            if match and insert:
                # one for array and one for insert
                matches_query.append(match)
                insert_query.append(insert)
            else:
                print("not possible for ",e )
            i += 1

    return [matches_query,insert_query]

# TODO Rewrite as a recursive function, taking elements n by n
def generate_bulk_query(elements, fnQueries):
    i = 1
    queries = []
    nb = len(elements)
    print("len = ", nb)


    for e in elements:

        #print("i % = ", i % 10)
        nb -= 1
        if (nb == 0):
            yield queries
        elif (i % 10 == 0):

            # by default we try with start with first priority ...
            query = fnQueries(e, 1, i)

            # exist and/or possible
            if query:
                # one for array and one for insert
                queries.append(query)
            else:
                print("not possible for ", e)

            yield queries
            queries = []
            i = 1

        else:
            query = fnQueries(e, 1, i)

            # exist and/or possible
            if query:
                queries.append(query)
            else:
                print("not possible for ", e)
            i += 1

    return queries

def insert_airports(airports):

    c_generator = generate_bulk_insert(airports, airport_query)

    for Matches, Inserts in c_generator:
        query = WOQLQuery().when(
            WOQLQuery().woql_and(*Matches),
            WOQLQuery().woql_and(*Inserts),)

        try:
           print("INSERT NEXT TEN")
           with wary.suppress_Terminus_diagnostics():
               query.execute(client)
        except Exception as e:
           print("EXCEPTION")
           wary.diagnose(e)

def insert_countries(countries):

    c_generator = generate_bulk_insert(countries, country_query)

    for Matches, Inserts in c_generator:

        query = WOQLQuery().when(
            WOQLQuery().woql_and(*Matches),
            WOQLQuery().woql_and(*Inserts))

        try:
           #with wary.suppress_Terminus_diagnostics():
           query.execute(client)
        except Exception as e:
           print("EXCEPTION")
           wary.diagnose(e)

# def insert_data(flights, client):
#
#     from load_files import load_countries
#
#
#     byfile_airport_flights = flights[0]
#     airport_flights = byfile_airport_flights[0]
#     first_flights = airport_flights[0]
#
#     query = WOQLQuery().when(True).woql_and(
#         WOQLQuery().cast(first_flights.airline_icao, "xsd:string", "v:airline_icao"),
#         WOQLQuery().insert("scm:Airline", "Airline").
#         property("icao_code",  "v:airline_icao"),
#     )
#
#     try:
#         with wary.suppress_Terminus_diagnostics():
#             query.execute(client)
#     except Exception as e:
#         print("EXCEPTION")
#         wary.diagnose(e)


# Todo : recursive way to find a valid code for a country
# Todo : Divide and conquer, firs try with priority 1 and if is not available,
#  try to relaunch failed query with priority 2
def get_country_code(airport, priority, id):

    original_name = airport.country
    name = original_name.replace(" ","\\s*").replace("(","\\(").replace(")","\\)")

    def unifying(variable):
        return variable+str(id)
    property_name= getPriority(priority)

    # in batch , query need to be unique, so we concat with str
    query = WOQLQuery().woql_and(WOQLQuery().triple(unifying("v:Country"), "type", "scm:Country"),
                                 WOQLQuery().triple(unifying("v:Country"), "scm:country_name", unifying("v:country_name")),
                                 WOQLQuery().triple(unifying("v:Country"), "scm:country_id", unifying("v:country_id")),
                                 WOQLQuery().triple(unifying("v:Country"), "scm:{p}".format(p=property_name),unifying("v:{v}".format(v=property_name))),
                                 WOQLQuery().re("(.*{re_name}.*)+".format(re_name=name),unifying("v:country_name"),[unifying("v:All"),unifying("v:Paren1")]))
    return [airport, query]

def exec_bulk_country_query(country_queries, priority):

    airport, country_queries_unzip = list(zip(*country_queries))

    query = WOQLQuery().woql_and(*country_queries_unzip)

    try:
        # with wary.suppress_Terminus_diagnostics():
        result = query.execute(client)
        df_result = pd.DataFrame() if is_empty(result) else wdf.query_to_df(result)
        if not df_result.empty:
            df_result['id'] = df_result.index
            columns = ["All", "Paren1", "Country", "country_id", getPriority(priority),"country_name"]
            df_result = pd.wide_to_long(df_result, columns , i="id", j="val").reset_index()

            return(zip(airport, df_result["country_id"]))

       ## TODO: Waiting that opt() work to catch bad query and rerun them.
    except Exception as e:
       print("EXCEPTION")
       wary.diagnose(e)

    # try:
    #    #with wary.suppress_Terminus_diagnostics():
    #    result = query.execute(client)
    #
    #    df_result = pd.DataFrame() if is_empty(result) else wdf.query_to_df(result)
    #
    #    if len(df_result.index) == 0 :
    #
    #        print("FAILED FOR THIS ONE" , original_name )
    #        return [None,None]
    #    if len(df_result.index) > 1 :
    #        #print("MORE THAN ONE, REQUIRE MORE FILTERING")
    #        #print(df_result)
    #        df_result_filtered = df_result.loc[df_result['country_name'].str.len() == len(original_name)]
    #        #print(df_result_filtered)
    #        return [df_result_filtered["country_id"].iloc[0], df_result_filtered[property_name].iloc[0]]
    #    else:
    #        pass
    #        #print("OK FOUND IT")
    #        #print(df_result)
    #
    #        return [df_result["country_id"].iloc[0], df_result[property_name].iloc[0]]
    #
    # except Exception as e:
    #    print("EXCEPTION")
    #    wary.diagnose(e)

def getPriority(id):
        priority = {1: "country_fips_code",
     2: "country_iso_code"}
        return priority[id]

if __name__ == "__main__":

    pd.set_option('display.max_columns', 20)
    pd.set_option('display.width', 500)

    #dataPath = Path("/home/reyman/Data/FlightRadar/")

    server_url = "https://terminusdb.unthinkingdepths.fr/"
    dbId = "flightDB"
    key = "unpetitouistiti"
    dburl = server_url + "/" + dbId

    client = woql.WOQLClient()
    try:
        print("[Connecting to the TerminusDB server..]")
        with wary.suppress_Terminus_diagnostics():
            client.connect(server_url, key)
    except Exception as e:
        print("[TerminusDB server is apparently not running?]")
        wary.diagnose(e)
    try:
        print("[Removing prior version of the database,  if it exists..]")
        with wary.suppress_Terminus_diagnostics():
            client.deleteDatabase(dbId)
    except Exception as e:
        print("[No prior database to delete]")
    try:
        print("[Creating new database..]")
        with wary.suppress_Terminus_diagnostics():
            client.createDatabase(dbId, "flightDB", key=None, comment="Result of scraping from flightradar website")
    except Exception as e:
        wary.diagnose(e)

    #mgd = MongoDBUtils(dataPath)
    #files = ["2019-03-28T03:12:40.544462+00:00.gz"]
    #data = mgd.loadDataFromMongoDB(files,None)

    from load_files import *

    create_schema(client)

    countries = load_countries()
    insert_countries(countries)

    #country_id, country_code = get_country_code(airport, airport_priority_code, 1)

    airports = load_airport()
    # return a generator with array of ten queries asking country code
    c_generator = generate_bulk_query(airports, get_country_code)

    i = 0

    for c in c_generator:
        #print(c ,"\n")
        # TODO : add recursivity
        result = exec_bulk_country_query(c, 1)

        if result:
            for airport, country_id in result:
                print("update Airport object with correct country_id = ", country_id)
                airport.country_id = country_id

        i += 1

    # filter airport without country_id
    # TODO : Commit avant de casser.
    # TODO : Fonctions recursives qui consomment les aéroports/country plutot qu'utiliser un modulo ?
    # TODO : Utiliser un cache pour éviter de refaire 1000 fois la meme requete,
    #  si existe dans le dictionnaire, alors pas besoin de faire la requete...

    insert_airports(airports)

    # inutile de refaire toute les query ...
    airlines = load_airlines()


    #insert_flights(data, client)