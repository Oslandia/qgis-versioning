.. include:: globals.rst

--------------------
Plugin functionality
--------------------

Summary
=======

Depending on the type of layer group (unversioned/versioned PostGIS database, PostGIS/Spatialite working copy) the |plugin| provides a different set of functionalities, as summarized in the following table.

   =================  ==============  ============  ==================== ====================
   Icon               Unversioned     Versioned     Working copy (PG/SL) Definition
   =================  ==============  ============  ==================== ====================
   |branch_png|                       X                                  Branching
   |checkout_png|                     X                                  Checkout Spatialite
   |checkout_pg_png|                  X                                  Checkout PostGIS
   |commit_png|                                     X                    Commit changes
   |help_png|         X               X             X                    Help
   |historize_png|    X                                                  Start versioning
   |update_png|                                     X                    Check if working copy up to date
   |view_png|                         X                                  View revisions
   =================  ==============  ============  ==================== ====================

The following table shows the combination of plugin messages and icons as a function of group type selected in the QGIS legend.

   =================  ========================   ====================
   Group type         Menu                       Comments
   =================  ========================   ====================
   No group           |no_group_selected_png|    No group item is selected in the legend
   Unversioned        |unversioned_menu_png|     Only option is to historize (green V)
   Versioned          |versioned_menu_png|       Textinfo (left) : DB schema branch= rev=
   Working Copy (SL)  |working_copy_sl_png|      Textinfo (left) : filename working rev=
   Working copy (PG)  |working_copy_pg_png|      Textinfo (left) : DB schema working rev=
   Mixed layers       |layers_not_same_db_png|   Layers in group do not share the same database or schema
   Same Name          |groups_same_name_png|     Groups must have different names
   Empty group        |empty_group_png|          Selected group is empty
   =================  ========================   ====================

Typical workflow
================

The following sections present how the |plugin| is used in |qg|.  Another section will detail what the plugin does in the database.

Unversioned database
++++++++++++++++++++

The sequence of operations begins with data in a PostGIS (PG) database that is loaded in a QGIS layer group.

.. note::
   The |plugin| operates on QGIS layer groups, not on individual layers.

At that stage the data is unversioned and the only option on the layer group, except for help, is to "historize" the database.

|unversioned_menu_png|

Clicking on the historize button (|historize_png|) will generate a warning from the plugin that four new columns will be added to all tables in the selected database.

|historize_warning_png|

As the warning mentions, *all* datasets in the database will be  versioned even though a subset of datasets (tables) was initially selected by the user to be loaded as layers in |qg|.

Upon accepting, the plugin creates a *versions* table in the formerly unversioned database schema, creates a new schema by appending "_trunk_rev_head" to the current schema name, indicating the original schema now has one line of versioning called "trunk", and loads the selected layers in a new |qg| group.  That is the begining of the versioning journey.

Versioned database
++++++++++++++++++

The plugin menu for a versioned layer group shows 5 icons.

|versioned_menu_png|

On the left, the name of the database schema is shown.  More specifically, the space-separated text items on the left identify four components :

- name of database
- name of schema
- name of branch (initial branch has default name *trunk*)
- revision number

Three operations can be performed on a versioned layer group :

#. branching
#. checking out a working copy
#. viewing specific revisions

Branching
*********

Branching involves the creation of a new schema in the database.  The new schema becomes another line of versioning of the original schema.

.. note::
   Even though branches are to hold independent versioining histories, they still result in a "commit" in the *revisions* table.

Checking out a working copy
***************************

Checking out a working copy creates one of two new layer groups, either a PG checkout (|checkout_pg_png|) or a spatialite checkout (|checkout_png|) :

- a spatialite layer group, called *working copy* (or named with the full path name of the spatialite file created if a group called "working copy" already exists) is created in the legend

- a PG layer group, the name of which is made up of the user provided schema name

In both cases, properties of individual layers in the groups will clearly show provenance (spatialite filename on the file system for spatialite checkouts or "schema"."view_name" for PG checkouts).

Viewing revisions
*****************

The view icon (|view_png|) shows the contents of the *revisions* table stored in the schema that was originally versioned.  The user can select one or more revisions by clicking the checkbox before the revision number.

|view_dialog_png|

Selecting one or more revision numbers will result in one group per revision created in the |qg| legend tree.  Each of those groups show all layers at the specific revision number.  The default name of those groups is "branch_name" + "revision" + the revision number, for example "trunk revision 1".

At the top of the dialog, a checkbox called "Compare selected revisions" gets enabled when two revisions are checked, allowing the user to compare between the two revisions.

|view_dialog_diff_mode_png|

Instead of getting two layer groups each containing all features, clicking on that checkbox creates a single group called "Compare revisions X vs Y" (with X < Y) where features that differ between the two compared revisions are given a rule-based symbology to highlight features that were created, updated or deleted.

|diff_mode_symbology_png|

A special case named "intermediate" identifies features that are "transient" items.  An example would be a point feature that would have been moved between the two revisions but which has a parent feature in revision X and a child feature in revision Y.

.. note::
   The "Compare selected revisions" checkbox is automatically unchecked and disabled if the number of selected revisions is not equal to two (2).

Working copy
++++++++++++

The |plugin| allows users to work on two types of working copies : PostGIS and Spatialite.  Once a database is versioned, users can *checkout* a working copy in either PostGIS or Spatialite formats.  In the former case, a new schema is created on the PostGIS server and a copy of the features for the selected layers is created.  In the latter, a local Spatialite file is created.

A peculiarity of spatialite checkouts is that features can be selected prior to checking out as explained in :doc:`spatialfiltering`.  Working copies can also be updated with changes committed by other users in the central PG database.

The following image shows the three icons found in the menu bar for a working copy, in this case of :

- a spatialite file

|working_copy_sl_png|

- PostGIS

|working_copy_pg_png|

On the left, either the name of the schema in the central database(for PG checkout) or the name of the spatialite file appears together with the current revision number.  The two basic operations that can be performed on a working copy are to either update (|update_png|) or to commit changes (|commit_png|).

Updating implies synchronizing the current working copy with the central database.  If the current working copy is behind the central database data is uploaded to the working copy.  Data is either integrated directly in the working copy or a conflict resolution workflow is launched in the event local edits conflict with database revisions newer than that at which the working copy was checked out.

Committing changes is rather self explanatory.  After changes were made in the working copy, they can be committed to the central database where they will be integrated and the revisions table updated with a new rev count.

Plugin group types in pictures
==============================

The following screenshots illustrate the various layer groups produced by the |plugin|.  Note that default group names (e.g. "working copy" for spatialite checkout) were modified to be more expressive.

- Unversioned database

|unversioned_png|

- Versioned database

|versioned_png|

- Spatialite checkout

|sl_checkout_png|

- PostGIS checkout

|pg_checkout_png|

- Branch

|branch_group_png|

- View revision (full mode)

|full_mode_view_png|

- View revision (diff or comparison mode)

|diff_mode_view_png|

Database artifacts
==================

Should I fill that section of just write pull-quotes specifying what happens on the DB side the in sections above ?
