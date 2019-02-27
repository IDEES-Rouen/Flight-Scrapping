from flightClass import *
from database import DatabaseCursor

countries_filename = "./resources/airports.dat"
db = DatabaseCursor('resources/seb_local.json')

countries = []

with open(countries_filename, 'r') as f:
    for line in f:
        try:
            item = line.replace('"', '').split(",")
            country = Countries(name= item[0], fipscode= item[1], iso3166code= item[2])

            if (item[2] != "\\N"):
                countries.append(country)

        except:
            print("")

with db as curs:

    for country in countries:
        print(country.iso3166code)
        curs.execute("""INSERT INTO countries (c_name, c_isocode,c_fipscode)
        VALUES (%s, %s, %s)""", (country.name, country.iso3166code, country.fipscode))

    db.conn.commit()
