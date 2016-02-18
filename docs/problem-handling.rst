.. include:: globals.rst

--------------------
When problems occur
--------------------

Multiple modifications to the same dataset amongst a number of users inevitably bring about conflicts.  This section shows how editing conflicts are managed by the plugin.  Specific errors or exceptions are also mentioned.

.. _conflict-resolution:

Conflict management
===================

For users to be able to commit modifications to the database, their working copy must be up to date with the database.  Take the example of two users checking out SL working copies from HEAD revision X.  Each on their workstations, they are editing features in their working copy.  One of the users will inevitably commit his(her) changes to the database first.  This will create another entry in the *revisions* table, that is increment the HEAD revision to X + 1.

When the second user tries to commit, a message will warn that the working copy needs to be updated :

|late_by_warning_png|

.. note::
  It is wise to always update a working copy before trying to commit. Updating directly by clicking |update_png| will check if the working copy is up to date.  If it is, a message will let you know : |uptodate_png|

  Else, the working copy is updated to the latest revision, for example |late_by_png|

If edits made by the first committer do not conflict with the second user's working copy, updating will proceed normally.  Changes made by the other user will be merged into the second user's working copy.  All is well and the second user can now commit his(her) changes.

In the event there are conflicting edits, the second user will be presented with this message after updating :

|conflict_warning_png|

A new layer is created by the plugin for every dataset that contain errors and it is displayed in the working copy layer group.  The name of that layer is made up of the original layer name to which the "_conflicts" string is appended :

|conflict_layer_png|

The figure above shows two conflicting features in one layer.  Highlighted is the edit the second user is trying to commit.  The attributes table shows the id of the conflicting feature.  The conflict is resolved by deleting the unwanted entry in the conflict layer, either 'mine' or 'theirs' and saving the edits.  On deletion of 'mine', the working copy edition is discarded; on deletion of 'theirs' the working copy edition is appended to the feature history (i.e. the working copy feature becomes a child of the last state of the feature in the historized database).

Once the conflict table is empty, committing can proceed.

.. note::
   On deletion of one conflict entry, both entries are removed (by a trigger) but the attribute table (and map canvas) are not refreshed. As a workaround, the user can close and re-open the attribute table to see the actual state of the conflict table.

   Since deleting 'mine' implies discarding one's own change, then committing will result in no change being committed.

   A useful tip if you deleted the wrong one (e.g. 'theirs' and you meant to delete 'mine') **and** did not save yet : CTRL-Z (undo) to the rescue.

In the more general case, multiple editions can be made to the same feature. Therefore child relations must be followed to the last one in order to present the user with the latest state of a given conflicting feature.

.. warning::
   Known bug : Updating a working copy may indicate it is up to date when in fact it is not.  This may happen if for example the checkout (working copy creation) was done on trunk and then another branch was created afterwards.  The working copy gets "stuck" at the latest revision of the branch it was checked out from.  The only way to get around this is to checkout a fresh working copy from the desired branch.  Edits made in the other working copy are still there and need to be integrated manually in the most recent working copy.

   Tip : clicking on the view icon |view_png| will tell you what the latest revision number is.

Errors and exceptions
=====================

Although errors are generally managed within the plugin, specific circumstances may trigger errors or Pyhton exceptions.  This section shows some of those errors and how they can be avoided or recovered from.
