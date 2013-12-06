Versioning
==========

Create a versionned database
----------------------------

cd /appropriate/path/to/valcea/versioning
createdb valcea
psql valcea -c 'create extension postgis'
psql valcea -f epanet_test_db.sql

Build and install the qgis plugin
---------------------------------

cd /appropriate/path/to/valcea/versioning
mkdir build
cd build
cmake .. && make
ln -s $PWD $HOME/.qgis2/python/plugins/versioning

Use the plugin in qgis
----------------------

FOnce the test database have been created, open qgis.

Activate the plugin 'versioning' in the plugin manager.

Load a posgis layer from the schema epanet_trunk_rev_head of test database.

Click on the 'work offline' icon (either in the plugin menu or in the plugin toolbar). Choose a file to save your layers locally.

Modify your layers.

Click on the 'commit' icon.

Please note that you must be able to access the database without password for the moment.

