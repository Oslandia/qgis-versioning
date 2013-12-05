import ogr

datasource = ogr.Open("PG:dbname='valcea' active_schema=epanet host='localhost' port='5432' user='vmo' password='toto' ")

datasink = ogr.Open("SQLite toto.sqlite")




