.. include:: globals.rst

----------------------------------
Selecting features before checkout
----------------------------------

Before checking out a local spatialite working copy, one can select features from the versioned tables to work on locally.  Any layer in the group can have features selected.  In the case of a layer with no features selected, the whole dataset will be checked out by the |plugin|.  This allows users to select only those features they are interested in editing rather than the whole dataset.

.. note::
 Feature selection prior to checkout only applies to spatialite checkouts.  It has yet to be implemented for PG checkouts.

Procedure
=========

- For each layer in the group, select features you want checked out.  This can be done in a number of ways in |qg|.
- When ready to checkout, click on the layer group and click on the spatialite checkout button (|checkout_png|).  At that point any layer with selected features will pop this warning to let the user know a subset of features will be checked out :

|selected_features_warning|

- Complete the rest of the default spatialite checkout workflow and check that only a subset of features was retrieved for the layers you selected features for.

In our example, only the selected polygons (yellow above) and all points (since no feature selection was performed on the point layer) were checked out :

|selected_features_local|
