.. include:: globals.rst

------------
Requirements
------------

This section is a list of both hard and soft requirements (aka suggestions).

Hard requirements
=================
Requirements considered 'hard' are those you cannot really live without.  The plugin may work but you may experience problems to make it work.  The two basic hard requirements are about |qg| and PostgreSQL/PostGIS.  Another pertains to naming conventions.

In a nutshell, those hard requirements are :

- QGIS 2.8+
- PG 9.x (ideally 9.2+)
- names asked by he plugin must begin with either a "_" (underscore) or a letter followed by any lowercase non accented letter, digit or underscore without spaces and up to 63 characters long

QGIS
+++++

Recent versions of the plugin were tested with |qg| 2.8.  It is worth mentioning that plugin versions < 0.2 were developed for older versions of |qg|, which may incidentally ship with older versions of spatialite/SQlite.  As of version 0.2 (Aug 2015), `spatialite <https://www.gaia-gis.it/fossil/libspatialite/index>`_ version 4.x is supported by the plugin.  This in turn makes any |qg| versions that come with older spatialite versions unusable with the plugin (for spatialite checkouts at least).

Another key dependency of the plugin is `ogr2ogr <http://www.gdal.org/ogr2ogr.html>`_.  Although the plugin does not depend on the most recent features of ogr2ogr, it is wise to stick to the version bundled in |qg| 2.8+ (2.8+ because newer versions of ogr2ogr shipped in newer versions of |qg| should be backwards compatible).

PostgreSQL/PostGIS
++++++++++++++++++

The plugin should work on any minor revision of the PostgreSQL 9.x series.  It was tested successfully on PostgreSQL 9.2 and 9.4.

There are no hard requirements on PostGIS as such on the part of the plugin.  Packing the most recent version supported in your PostgreSQL installation should be sufficient.

Naming conventions
++++++++++++++++++

Operation of the plugin is best ensured by sticking to the PostgreSQL naming rules.  As suggested `here <http://www.informit.com/articles/article.aspx?p=409471>`_ :

.. pull-quote::
   PostgreSQL uses a single data type to define all object names: the name type.  A value of type name is a string of 63 or fewer characters. A name must start with a letter or an underscore; the rest of the string can contain letters, digits, and underscores.

.. warning::
   Do NOT use empty spaces in any identifier the plugin asks you to supply.

   Do NOT use accented characters (e.g. German umlaut or French "accent aigu")

   For optimal operation, avoid using spaces in the full path name of files intended to be used by the plugin, for example spatialite files.

Even though PostgreSQL object names can contain capital letters, the plugin does not currently support object names other than in lowercase letters (plus digits and underscores as mentioned above).  Even though the plugin ensures some level of protection in that respect, it is best to stick to those conventions when naming a new PG checkout (see later for an explanation), a branch or any other name the plugin asks you to provide.

The same applies to spatialite filenames (SL checkout).

Soft requirements
=================
Soft requirements are more like "best practice" suggestions.  As the saying goes : Your Mileage May Vary.

Separate schema
+++++++++++++++

As will be explained in more detail later in this document, the |plugin| operates "historization" by adding columns to each table in a particular database together with a *revisions* table that holds all revision information.  For a number of reasons, it is wise to isolate your geographic data in a schema **other** than the *public* schema.

As mentioned `here <http://blog.cleverelephant.ca/2010/09/postgis-back-up-restore.html>`_ :

.. pull-quote::
   "... store no data in the 'public' schema."

The specific context of the previous quote pertains to backup and restore procedures but the advice also applies for the |plugin|.
