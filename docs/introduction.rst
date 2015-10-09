.. include:: globals.rst

-------------------------------
Introductory text
-------------------------------

Summary
=======

The |plugin| allows many users to collaborate on geographical features stored in PostGIS databases.  The typical use case met by the plugin involves checking out a local working copy to edit features in (potentially) offline mode using SpatiaLite.  Changes are then committed back to the PostGIS server.  The `original documentation`_ explains all supported use cases thoroughly.

Spatial extent
==============

One item the `original documentation`_ does not detail however is how to select a subset of features, either spatially (bounding box, user hand-picking or selecting through the |qg| interface, etc.) or by filtering on attributes.

Explain when we know how to do that : DB Manager ?
