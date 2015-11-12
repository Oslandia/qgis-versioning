.. include:: globals.rst

--------------------
Plugin functionality
--------------------

Summary
=======

Depending on the type of layer group (source PostGIS schema or working copy) the |plugin| provides a different set of functionalities, as summarized in the following table.

   =================  ==============  ============  ==============  ===================
   Icon               Unversioned PG  Versioned PG  Working copy    Definition
   =================  ==============  ============  ==============  ===================
   |branch_png|                       X                             Branching
   |checkout_png|                     X                             Checkout Spatialite
   |checkout_pg_png|                  X                             Checkout PostGIS
   |commit_png|                                     X               Commit changes
   |help_png|         X               X             X               Help
   |historize_png|    X                                             Start versioning
   |update_png|                                     X               Check if up to date
   |view_png|                         X                             View revision table
   =================  ==============  ============  ==============  ===================

Layer group types
=================

The sequence of operations begins with unversioned PostGIS (PG) tables that are "historized" (versioned).  Once versioned, feature sets (technically view in versioned schemas) can be checked out to be eiddted locally either as a PG or a spatialite working copy.  Changes made in a working copy can then be committed back to the versioned schema from which it originates.  Working copies can also be updated with changes committed by other users in the central PG database.

Unversioned schemas
===================

PG layers, which at that stage are unversioned, need to be added to a |qg| layer group.  After all layers have been added, clicking on the group legend item will trigger a refresh of the plugin menu.  Aside from the help icon, which is always available, the only option for unversioned PG layers is to historize, that is begin the versioning journey.

|unversioned_menu_png|

Clicking on the historize button (|historize_png|) will generate a message from the plugin warning you that four new columns will be added to all tables in the selected database.

|historize_warning_png|

Upon accepting, the plugin creates a *versioning* table in the formerly unversioned database schema, creates a new schema by appending "_trunk_rev" to the current schema name and loads the selected layers in a new |qg| group.

Versioned schemas
=================

The plugin menu for a versioned layer group shows 5 icons.

|versioned_menu_png|

On the left, the name of the dtabase table is shown.  The two basic operations that can be performed on a versioned layer group are to branch (|branch_png|) or to checkout a working copy.  For the former, a new schema in the central database is created.  For the latter, a *working copy* group is created to host either a PG checkout (|checkout_pg_png|) or a spatialite checkout (|checkout_png|).  The view icon (|view_png|) simply shows the contents of the *revisions* table stored in the schema that was originally versioned.

Working copy
============

The plugin menu for a working copy layer group shows 3 icons.

|working_copy_png|

On the left, either the name of the branch in the central database or the name of the spatialite file appears together with revision number.  The two basic operations that can be performed on a working copy are to either update (|update_png|) or to commit changes (|commit_png|).

Etc.
