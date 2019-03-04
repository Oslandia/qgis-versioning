FROM debian:buster

RUN apt-get update \
    && apt-get install -y postgresql-11-postgis-2.5 libsqlite3-mod-spatialite python3-psycopg2 gdal-bin

# to be able to connect locally
RUN echo "host all all 127.0.0.1/32 trust" > /etc/postgresql/11/main/pg_hba.conf

COPY . qgis-versioning
WORKDIR qgis-versioning/test

CMD service postgresql start && python3 tests.py 127.0.0.1 postgres -v