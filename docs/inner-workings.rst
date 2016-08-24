.. include:: globals.rst

--------------------
Inner workings
--------------------

This section is intended to explain more technical aspects of the versioning implemented by the |plugin|.

Central database
================

Once a database is versioned, three operations can be performed on table rows: INSERT, DELETE and UPDATE. To be able to track history, every row is kept in the tables. Deleted rows are marked as such and updated rows are a combined insertion-deletion where the deleted and added rows are linked to one another as parent and child.

A total of five columns are needed for versioning the first branch:

**PRIMARY KEY**
    a unique identifier across the table
**branch_rev_begin**
    revision when this record was inserted
**branch_rev_end**
    last revision for which this record exists (i.e. revision when it was deleted minus one)
**branch_parent**
    in case the row has been inserted as a result of an update, this field stores the id of the row that has been updated
**branch_child**
    in case the row has been marked as deleted as a result of an update, this field stores the id of the row that has been inserted in its place.

For each additional branch, four additional columns are needed (the ones with the prefix 'branch\_').

.. note::
   A null value for *branch_rev_begin* means that a row (feature) does not belong to that branch.

SQL views are used to see a snapshot of the database for a given revision number. Noting 'rev' the revision we want to see, the condition for a row to be present in the view is:

    (*branch_rev_end* IS NULL OR *branch_rev_end* >= rev) AND *branch_rev_begin* <= rev

In the special case of the latest revision, or head revision, the condition reads:

    *branch_rev_end* IS NULL AND *branch_rev_begin* IS NOT NULL

.. note::
   Since elements are not deleted (but merely marked as such) in an historized table, care must be taken with the definition of constraints, in particular the conceptual unicity of a field values.

Views for revisions must be read-only and historized tables should **never** be edited directly. This is a basic principle for version control : editions must be made to working copies an then committed to the database. Please note that by default PostGIS 9.3 creates updatable views.


Working copy database
=====================

For each versioned table in the working copy, a view is created with the suffix _view (e.g. mytable_view). Those views typically filter out the historization columns and show the head revision. A set of triggers is defined to allow operating on those views (DELETE, UPDATE and INSERT).

The DELETE trigger simply marks the end revision of a given record.

The INSERT trigger creates a new record and fills the *branch_rev_begin* field.

The UPDATE trigger creates a new record and fills the *branch_rev_begin* and *branch_parent* fields. It then marks the parent record as deleted, and fills the *branch_rev_end* and *branch_child* fields accordingly.
