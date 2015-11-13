.. include:: globals.rst

--------------------
Plugin functionality
--------------------

Summary
=======

Depending on the type of layer group (source PostGIS schema or working copy) the |plugin| provides a different set of functionalities, as summarized in the following table.

   =================  ==============  ============  ==============  ====================
   Icon               Unversioned PG  Versioned PG  Working copy    Definition
   =================  ==============  ============  ==============  ====================
   |branch_png|                       X                             Branching
   |checkout_png|                     X                             Checkout Spatialite
   |checkout_pg_png|                  X                             Checkout PostGIS
   |commit_png|                                     X               Commit changes
   |help_png|         X               X             X               Help
   |historize_png|    X                                             Start versioning
   |update_png|                                     X               Check if up to date
   |view_png|                         X                             View revisions table
   =================  ==============  ============  ==============  ====================

Layer group types
=================

The sequence of operations begins with unversioned PostGIS (PG) tables that are "historized" (versioned).  Once versioned, feature sets (technically views in schemas) can be checked out to be edited locally either as a PG or a spatialite working copy.  Changes made in a working copy can then be committed back to the schema from which it originates.  Working copies can also be updated with changes committed by other users in the central PG database.

Unversioned database
====================

PG tables, which at that stage are unversioned, need to be added to a |qg| layer group.  After all tables have been added as layers, clicking on the group legend item will trigger a refresh of the plugin menu.  Aside from the help icon, which is always visible, the only option for unversioned PG layers is to historize, that is begin the versioning journey.

|unversioned_menu_png|

Clicking on the historize button (|historize_png|) will generate a warning from the plugin that four new columns will be added to all tables in the selected database.

|historize_warning_png|

Upon accepting, the plugin creates a *versions* table in the formerly unversioned database schema, creates a new schema by appending "_trunk_rev_head" to the current schema name, indicating the original schema now has one line of versioning called "trunk", and loads the selected layers in a new |qg| group.

Versioned database
==================

The plugin menu for a versioned layer group shows 5 icons.

|versioned_menu_png|

On the left, the name of the database schema is shown.  The two basic operations that can be performed on a versioned layer group are to branch (|branch_png|) or to checkout a working copy.  For the former, a new schema in the central database is created.  The new schema becomes another line of versioning of the original schema.  For the latter, a *working copy* group is created to host either a PG checkout (|checkout_pg_png|) or a spatialite checkout (|checkout_png|).  The view icon (|view_png|) simply shows the contents of the *revisions* table stored in the schema that was originally versioned.

Working copy
============

The plugin menu for a working copy layer group shows 3 icons.

|working_copy_png|

On the left, either the name of the schema in the central database(for PG checkout) or the name of the spatialite file appears together with the current revision number.  The two basic operations that can be performed on a working copy are to either update (|update_png|) or to commit changes (|commit_png|).

Updating implies synchronizing the current working copy with the central database.  If the current working copy is behind the central database data is uploaded to the working copy.  Data is either integrated directly in the working copy or a conflict resolution workflow is launched in the event local edits conflict with database revisions newer than that at which the working copy was checked out.

Committing changes is rather self explanatory.  After changes were made in the working copy, they can be committed to the central database where they will be integrated and the revisions table updated with a new rev count.
