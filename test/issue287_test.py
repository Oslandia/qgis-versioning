from .. import versioning
from pyspatialite import dbapi2
import psycopg2
import os
import shutil
import tempfile

if __name__ == "__main__":

    test_data_dir = os.path.dirname(os.path.realpath(__file__))
    tmp_dir = tempfile.gettempdir()

    # create the test database

    os.system("dropdb epanet_test_db")
    os.system("createdb epanet_test_db")
    os.system("psql epanet_test_db -c 'CREATE EXTENSION postgis'")
    os.system("psql epanet_test_db -f "+test_data_dir+"/issue287_pg_dump.sql")

    # try the update
    shutil.copyfile(test_data_dir+"/issue287_wc.sqlite", tmp_dir+"/issue287_wc.sqlite")
    versioning.update(tmp_dir+"/issue287_wc.sqlite", "dbname=epanet_test_db")
    versioning.commit(tmp_dir+"/issue287_wc.sqlite", "test message", "dbname=epanet_test_db")
