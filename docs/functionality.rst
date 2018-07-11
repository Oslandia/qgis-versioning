.. include:: globals.rst

--------------------
Plugin functionality
--------------------

In a nutshell
=============

Depending on the type of layer group (unversioned/versioned PostGIS database, PostGIS/Spatialite working copy) the |plugin| provides a different set of functionalities, as summarized in the following table.

   =======================  ===========  ============  ==================== ================================
   Icon                     Unversioned  Versioned     Working copy (PG/SL) Definition
   =======================  ===========  ============  ==================== ================================
   |branch_png|                             X                                  Branching
   |merge_png|                                         X                             Merging
   |checkout_png|                           X                                  Checkout Spatialite (SL)
   |checkout_pg_png|                        X                                  Checkout PostGIS (PG)
   |checkout_pg_local_png|                  X                                  Checkout PostGIS local (PGL)
   |commit_png|                                           X                    Commit changes
   |archive_png|                            X                                  Archiving
   |help_png|               X               X             X                    Help
   |historize_png|          X                                                  Start versioning (historize)
   |update_png|                                           X                    Check if working copy up to date
   |view_png|                               X                                  View revisions
   =======================  ===========  ============  ==================== ================================

The following table shows the combination of plugin text information and icons as a function of group type selected in the |qg| legend.

   =======================  ========================   ================================
   Group type               Menu                       Comments
   =======================  ========================   ================================
   No group                 |no_group_selected_png|    No group item selected in legend
   Unversioned              |unversioned_menu_png|     Only option : historize
   Versioned                |versioned_menu_png|       Textinfo : DB + schema + branch= + rev=
   Versioned (branch)       |versbranch_menu_png|      Textinfo : DB + schema + branch= + rev=
   Working Copy (SL)        |working_copy_sl_png|      Textinfo : filename + working + rev=
   Working copy (PG & PGL)  |working_copy_pg_png|      Textinfo : DB + schema + working rev=
   Mixed layers             |layers_not_same_db_png|   Layers in group do not share the same database or schema
   Same Name                |groups_same_name_png|     Groups must have different names
   Empty group              |empty_group_png|          Selected group is empty
   =======================  ========================   ================================

Typical workflow
================

The following sections present how the |plugin| is used in |qg|.  A later section will detail what the plugin does in the PostgreSQL database.

Unversioned database
++++++++++++++++++++

The sequence of operations begins with data in a PostGIS (PG) database that is loaded in a |qg| layer group.

.. note::
   The |plugin| operates on |qg| layer groups, not on individual layers.

At that stage the data is unversioned and the only option on the layer group, except for help, is to "historize" the database in the current schema.

|unversioned_menu_png|

Clicking on the historize button (|historize_png|) will generate a warning from the plugin that four new columns will be added to all tables in the selected database schema.

|historize_warning_png|

As the warning mentions, *all* datasets in the database will be  versioned even though a subset of datasets (tables) was initially selected by the user to be loaded as layers in |qg|.

Upon accepting, the plugin creates a *versions* table in the formerly unversioned database schema, creates a new schema by appending "_trunk_rev_head" to the current schema name, indicating the original schema now has one line of versioning called "trunk", and loads the selected layers in a new |qg| group.  That is the begining of the versioning journey.

.. note::
   The symobology of the original layers, if any, is not carried over to the versioned database layer group

Versioned database
++++++++++++++++++

The plugin menu for a versioned layer group shows 6 icons.

|versioned_menu_png|

The space-separated text items on the left identify four components :

- name of database
- name of schema
- name of branch (initial branch has default name *trunk*)
- revision number (or 'head' if latest)

Three operations can be performed on a versioned layer group :

#. checking out a working copy (SL , PG or PGL)
#. viewing specific revisions
#. branching
#. archiving

Checking out a working copy
***************************

Checking out a working copy creates one of three new layer groups, either a spatialite checkout (|checkout_png|), a PG checkout (|checkout_pg_png|) into the same database or a PG local checkout (|checkout_pg_local_png|) into an other database, typically on a local database:

- a spatialite layer group, called *working copy* (or named with the full path name of the spatialite file created if a group called "working copy" already exists) is created in the legend

- a PG layer group, the name of which is made up of the user provided schema name

In both cases, properties of individual layers in the groups will clearly show provenance (spatialite filename on the file system for spatialite checkouts or "schema"."view_name" for PG checkouts).

Working copies
--------------

The |plugin| allows users to work on two types of working copies : PostGIS (PG) and Spatialite (SL).  Once a database is versioned, users can *checkout* a working copy in either PostGIS or Spatialite formats.  In the former case, a new schema is created on the PostGIS server with the features for the selected layers in SQL views.  In the latter, a local Spatialite file is created.  SL working copies allow users to work offline, since the database is held in a file on the local machine.  PG working copies require a live access to the central database.

A peculiarity of spatialite working copies is that features can be selected prior to checking out as explained in :doc:`spatialfiltering`.  Working copies can also be updated with changes committed by other users in the central PG database.

The following image shows the three icons found in the menu bar for a working copy for :

- spatialite

|working_copy_sl_png|

- PostGIS (PG and PGL)

|working_copy_pg_png|

On the left, either the name of the schema in the central database (for PG checkout) or the name of the spatialite file appears together with the current working revision number.

.. note::
   The *working rev* number equals the latest revision number in the database (head) plus one.  The reason is that when work will be committed back to the central database it will be committed to revision = *working rev* if no other commits were made by other users in the meantime or whichever revision number > *working rev*.

The two basic operations that can be performed on a working copy are to either update (|update_png|) or to commit changes (|commit_png|).

Updating implies synchronizing the current working copy with the central database.  If the current working copy is not synchronized (late) with the central database, data is uploaded to the working copy.  Data is either integrated directly in the working copy or a  :ref:`conflict resolution <conflict-resolution>` workflow is launched in the event local edits conflict with database revisions newer than that at which the working copy was initially checked out.

As the name implies, committing involves uploading the changes to the central database where they will be integrated and the revisions table updated with a new record.

Viewing revisions
*****************

The view icon (|view_png|) shows the contents of the *revisions* table stored in the schema that was originally versioned.  The user can select one or more revisions by clicking the checkbox before the revision number.

|view_dialog_png|

Selecting one or more revision numbers will result in one group per revision being created in the |qg| legend tree.  Each of those groups show all layers at the specific revision number.  The default name of those groups is "branch_name" + "revision" + the revision number, for example "trunk revision 1".

At the top of the dialog, a checkbox called "Compare selected revisions" gets enabled when two revisions are checked, allowing the user to compare between the two revisions.

|view_dialog_diff_mode_png|

Instead of getting two layer groups each containing all features, clicking on that checkbox creates a single group called "Compare revisions X vs Y" (with X < Y) where features that differ between the two compared revisions are given a rule-based symbology as a function of whether they were created, updated or deleted.

|diff_mode_symbology_png|

A special case named "intermediate" identifies features that are "transient" items.  An example would be a point feature that would have been moved between the two revisions but which has a parent feature in revision X and a child feature in revision Y.

.. note::
   The "Compare selected revisions" checkbox is automatically unchecked and disabled if the number of selected revisions is not equal to two (2).

Branching
*********

Branching involves the creation of a new schema in the database.  The new schema becomes another line of versioning of the original schema.

.. note::
   Even though branches are to hold independent versioning histories, they still result in a "commit" in the *revisions* table.

Archiving
*********

Archiving consists of moving deleted data (trunk_rev_end) from version 1 to the selected version.

They are moved in a table with the same name in a new schema with the same schema name suffixed by _archive. The administrator can move this tables on a tablespace if he deems it useful

A view is created to find the table as if it had not been archived.

|archive_schemas_png|
=======
Merging
*******

As a reminder, the data between branches are the same, only the information on 4 columns added in the views differ.
Thus, a merge consists in putting back into trunk, the revision numbers of the columns _rev_begin and _rev_end branches. If trunk_rev_begin is empty it means it is an addition. If trunk_rev_begin exists and trunk_rev_end and null and branch_rev_end is not it is a deletion in the other branch.

The workflow must be branching, checkout from this branch, work into your working copy, commit into the branch, merging.

Plugin group types in pictures
==============================

The following screenshots illustrate the various layer groups produced by the |plugin|.  Note that default group names (e.g. "working copy" for spatialite checkout) were modified to be more expressive.

- Unversioned database

.. _unversioned-database:

|unversioned_png|

- Versioned database

.. _versioned-database:

|versioned_png|

.. note::
   The reason why there are fewer points for the versioned database has to do with feature duplication/multiplication in the original table.  See :ref:`commits-illustrated` for an illustration between the original table contents and the view generated by the historization process.

- Spatialite checkout

|sl_checkout_png|

- PostGIS checkout

|pg_checkout_png|

- Branch

|branch_group_png|

- View revision (full mode)

|full_mode_view_png|

.. note::
   The leftmost point at the top shows that at that revision (here 1) the feature was there.  It is not in the "head" view provided by the :ref:`versioned database <versioned-database>` view but it shows in the :ref:`unversioned database <unversioned-database>` because features are added continusouly in the tables once historization has begun.  See :ref:`commits-illustrated` for details.

- View revision (diff or comparison mode)

|diff_mode_view_png|

Database artifacts
==================

This section shows database artifacts created by the plugin and discusses users permissions in the context of the plugin.

Basic workflow : historization
++++++++++++++++++++++++++++++

First, we begin with an existing database and schema (in this case *epanet_test_db* and *epanet*, respectively) with two geodata tables : *junctions* (points) and *pipes* (lines).

|initial_db_png|

"Historizing" the database |historize_png| will add two things :

#. An additional table called *revisions* to the original schema
#. An additional schema called *epanet_trunk_rev_head* with N views (N being equal to the number of tables in the original schema)

|historization_png|

The additional schema's name is made up of the original schema plus the name of the branch, which defaults to 'trunk' for the first commit brought about by the historization process, plus the revision number, in this case "rev_head" because that schema hold the latest (*head*) revision.

Once the database is versioned, branches other than 'trunk' can be added.  Clicking on |branch_png| will present the user with a text box to supply the name of a new branch.  Here is the result for the name *mybranch*.

|creating_mybranch_png|

Note the added schema, called *epanet_mybranch_rev_head*, which also contains two views of the original dataset.

After adding the *mybranch* branch, the revisions table now shows two entries : one for the initial commit (historization) and another one for the new branch.

|revisions_table_png|

.. _commits-illustrated:

Committing data
+++++++++++++++

After the database is versioned, users can start checking out working copies and edit features.  When they are happy with their edits, they may "commit" their changes to create a new revision in the branch they have checked out from (usually 'trunk').  This is the pop up windows that appears at commit time :

|commit_ui_png|

The committer now has the option to choose another PG username if integrating data from another contributor.  See :ref:`user permissions <user_permissions>` for more detailed explanations of typical user roles in PostgreSQl.  After clicking OK and giving some time for the upload to get through, the user should see this :

|commit_success_png|

Let us look at an example.

Say a user edits the pipes dataset and commits the results a total of 4 times including the initial commit.  The *revisions* table will show 4 records.  If we look at the *pipes* view in the 'trunk_rev_head' schema, we see three features :

.. figure:: images/pipes_view.png

   Pipes view

Looking at the corresponding *pipes* table in the original schema, we find the whole story :

.. figure:: images/pipes_table.png

   Pipes table

Notice how the view only shows those features (pid = 7,8,9) beginning at the latest (HEAD) revision, number 4 in our case.  The table shows the details of the versioning history of the three features at HEAD revision :

- pid 1 started at rev 1 and ended at 2; it was edited to become pid 4
- pid 4 shows its parent is pid 1; it existed only at rev 3 to become pid 7
- pid 7 shows its parent is pid 4; it shows neither *trunk_rev_end* nor *trunk_child*, so it is at HEAD (latest revision)

----

- pid 2 existed at rev 2, then it was edited to become pid 5
- pid 5 shows its parent is pid 2; it existed only at revision 3 and became pid 8
- pid 8 shows its parent is pid 5; like pid 7, it is at HEAD revision

----

- pid 3 existed at rev 2, where it was edited to become pid 6
- pid 6 shows its parent is pid 3; it existed only at revision 3 and became pid 9
- pid 9 shows its parent is pid 6; like pid 7 and 8, it is at HEAD revision

More details can be found in the :doc:`inner-workings` section.

.. _user_permissions:

User permissions considerations
+++++++++++++++++++++++++++++++

Users of the plugin in QGIS must be able to create objects in the PostgreSQL database, like tables (e.g. the *revisions* table), schemas and views (e.g. to create new branches or PG checkouts).  The plugin requires database users to be owners of all tables in the source schema.  This is best achieved by configuring a group role with write access to the source schema as owner of all tables and put individual users in that group.

In the more recent versions of the plugin, another type of user with read-only operations may be used in the commit workflow.  In a typical organization, some users will be allowed to commit changes to the database while others may only either view the data or submit modifications of their own (e.g. in the form of a spatialite file) to a committer.  In the latter case, the committer has the opportunity to identify someone else as data editor at commit time.  This ensures some level of traceability as to who edited features and who committed (or approved or further modifed before final commit).  For this to be possible, all potential data editors must have a corresponding username in the database and ideally be associated with a read-only group role.

Although the plugin does not enforce a non null "Author" field in the *revisions* table (e.g. in tests), the current default behaviour of the plugin adds the following information in the "Author" field :

    OS:OS_username.committer_pg_username.[read-only]\_pg_username

*OS* specifies the committer's operating system (e.g. "Linux, MacOS10.10.5, Windows7").  *OS_username* is the username of the committer on his (her) workstation.  *committer_pg_username* defaults to the committers user name in PostgreSQL (not editable at commit) and *[read-only]_pg_username* is the bit of information in the the "Author" field that may be selected by the committer. It defaults to the current committers pg username.  We use square brackets to indicate that the committer may also select committer-level users as data editors.
