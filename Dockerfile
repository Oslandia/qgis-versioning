FROM debian:sid

RUN apt-get update \
    && apt-get install -y \
    gdal-bin \
    libsqlite3-mod-spatialite \
    postgresql-11-postgis-2.5 \
    python3-psycopg2 \
    qgis \
    xvfb

# to be able to connect locally
RUN echo "host all all 127.0.0.1/32 trust" > /etc/postgresql/11/main/pg_hba.conf

COPY . qgis-versioning
WORKDIR qgis-versioning/test

CMD ./run_tests.sh