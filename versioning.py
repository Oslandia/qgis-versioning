
"""
/***************************************************************************
 versioning
                                 A QGIS plugin
 postgis database versioning 
                              -------------------
        begin                : 2013-12-04
        copyright            : (C) 2013 by Oslandia
        email                : infos@oslandia.com
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

from PyQt4.QtCore import *
from PyQt4.QtGui import *
from qgis.core import *
import resources_rc
import os
import os.path
from pyspatialite import dbapi2 as db
import psycopg2
import commit_msg_ui
import connections_ui

qset = QSettings( "oslandia", "horao_qgis_plugin" )

WIN_TITLE = "versioning"

def escapeQuotes(s):
    return str.replace(str(s),"'","''");

class Versioning:
    def __init__(self, iface):
        # Save reference to the QGIS interface
        self.iface = iface
        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)

        self.qConnectionDialog = QDialog(self.iface.mainWindow())
        self.connectionDialog = connections_ui.Ui_ConnectionDialog()
        self.connectionDialog.setupUi(self.qConnectionDialog)

        self.qCommitMsgDialog = QDialog(self.iface.mainWindow())
        self.commitMsgDialog =commit_msg_ui.Ui_CommitMsgDialog()
        self.commitMsgDialog.setupUi(self.qCommitMsgDialog)

    def initGui(self):
        # Create action  checkout
        self.checkout_action = QAction(
            QIcon(":/plugins/versioning/checkout.svg"),
            u"checkout", self.iface.mainWindow())
        self.checkout_action.setWhatsThis("checkout")
        # connect the action to the run method
        self.checkout_action.triggered.connect(self.checkout)

        # Add toolbar button and menu item
        self.iface.addToolBarIcon(self.checkout_action)
        self.iface.addPluginToMenu( WIN_TITLE, self.checkout_action)

        self.update_action = QAction(
            QIcon(":/plugins/versioning/update.svg"),
            u"update", self.iface.mainWindow())
        self.update_action.setWhatsThis("update working copy")
        # connect the action to the run method
        self.update_action.triggered.connect(self.update)

        # Add toolbar button and menu item
        self.iface.addToolBarIcon(self.update_action)
        self.iface.addPluginToMenu( WIN_TITLE, self.update_action)

        # Create action commit
        self.commit_action = QAction(
            QIcon(":/plugins/versioning/commit.svg"),
            u"commit", self.iface.mainWindow())
        self.commit_action.setWhatsThis("commit modifications")
        # connect the action to the run method
        self.commit_action.triggered.connect(self.commit)

        # Add toolbar button and menu item
        self.iface.addToolBarIcon(self.commit_action)
        self.iface.addPluginToMenu( WIN_TITLE, self.commit_action)

    def unload(self):
        # Remove the plugin menu item and icon
        self.iface.removePluginMenu( WIN_TITLE, self.checkout_action)
        self.iface.removePluginMenu( WIN_TITLE, self.update_action)
        self.iface.removePluginMenu( WIN_TITLE, self.commit_action)
        self.iface.removeToolBarIcon(self.checkout_action)
        self.iface.removeToolBarIcon(self.update_action)
        self.iface.removeToolBarIcon(self.commit_action)

    def unresolvedConflicts(self):
        found = ""
        for name,layer in QgsMapLayerRegistry.instance().mapLayers().iteritems():
            uri = QgsDataSourceURI(layer.source())
            if layer.providerType() == "spatialite" and uri.table()[-5:] == "_view": 
                scon = db.connect(uri.database())
                scur = scon.cursor()
                table = uri.table()[:-5] # remove suffix _view
                scur.execute("SELECT tbl_name FROM sqlite_master WHERE type='table' AND tbl_name = '"+table+"_conflicts'")
                table_conflicts = scur.fetchone()
                if table_conflicts:
                    scur.execute("SELECT * FROM "+table_conflicts[0])
                    if scur.fetchone(): found += table+", "
                    else: scur.execute("DROP  TABLE "+table_conflicts[0])
                scon.commit()
                scon.close()
            
        if found: 
            QMessageBox.warning( self.iface.mainWindow(), "Warning", "Unresolved conflics for layer(s) "+found[:-2]+".\n\nPlease resolve conflicts by deleting either 'mine' or 'theirs' in the conflict layers before continuing.")
            return True
        else:
            return False

    def update(self):
        """merge modifiactions since last update into working copy"""
        print "update"
        if self.unresolvedConflicts(): return
        # get the target revision from the spatialite db
        # create the diff in postgres
        # load the diff in spatialite
        # detect conflicts
        # merge changes and update target_revision
        # delete diff
        versionned_layers = {}
        for name,layer in QgsMapLayerRegistry.instance().mapLayers().iteritems():
            uri = QgsDataSourceURI(layer.source())
            if layer.providerType() == "spatialite" and uri.table()[-5:] == "_view": 
                versionned_layers[name] = layer

        if not versionned_layers: 
            print "No versionned layer found"
            QMessageBox.information( self.iface.mainWindow(), "Notice", "No versionned layer found")
            return
        else:
            print "updating ", versionned_layers

        all_max_rev = 0
        for name,layer in versionned_layers.iteritems():
            conflict_for_this_layer = False
            uri = QgsDataSourceURI(layer.source())
            scon = db.connect(uri.database())
            scur = scon.cursor()
            table = uri.table()[:-5] # remove suffix _view
            scur.execute("SELECT rev, branch, table_schema, conn_info "+
                "FROM initial_revision "+
                "WHERE table_name = '"+table+"'")
            [rev, branch, table_schema, conn_info] = scur.fetchone()

            pcon = psycopg2.connect(conn_info)
            pcur = pcon.cursor()
            pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions WHERE branch = '"+branch+"'")
            [max_rev] = pcur.fetchone()
            all_max_rev = max (max_rev, all_max_rev)
            if max_rev == rev: 
                print "Nothing new in branch "+branch+" in "+table+"."+table_schema+" since last update"
                continue

            # create the diff
            diff_schema = table_schema+"_"+branch+"_"+str(rev)+"_to_"+str(max_rev)+"_diff"
            pcur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"+diff_schema+"'")
            if not pcur.fetchone():
                pcur.execute("CREATE SCHEMA "+diff_schema)

            pcur.execute("SELECT column_name "+
                    "FROM information_schema.columns "+
                    "WHERE table_schema = '"+table_schema+"' AND table_name = '"+table+"'")
            cols = ""
            for c in pcur: 
                if c[0] != "geom": cols += c[0]+", "
            cols = cols[:-2] # remove last coma and space

            pcur.execute("SELECT srid, type "+
                "FROM geometry_columns "+
                "WHERE f_table_schema = '"+table_schema+"' AND f_table_name ='"+table+"' AND f_geometry_column = 'geom'")
            [srid, geom_type] = pcur.fetchone()
            pcur.execute( "DROP TABLE IF EXISTS "+diff_schema+"."+table+"_diff")
            pcur.execute( "CREATE TABLE "+diff_schema+"."+table+"_diff AS "+
                    "SELECT "+cols+", geom::geometry('"+geom_type+"', "+str(srid)+") AS geom "+
                    "FROM "+table_schema+"."+table+" "+
                    "WHERE "+branch+"_rev_end = "+str(rev)+" OR "+branch+"_rev_begin > "+str(rev))
            pcur.execute( "ALTER TABLE "+diff_schema+"."+table+"_diff "+
                    "ADD CONSTRAINT "+table+"_"+branch+"__hid_pk PRIMARY KEY (hid)") 
            pcon.commit()

            scur.execute( "DROP TABLE IF EXISTS "+table+"_diff")
            scon.commit()

            # import the diff to spatialite
            cmd = "ogr2ogr -preserve_fid -f SQLite -update "+uri.database()+" PG:\""+conn_info+" active_schema="+diff_schema+"\" "+table+"_diff"
            print cmd
            os.system(cmd)

            # cleanup in postgis
            pcur.execute("DROP SCHEMA "+diff_schema+" CASCADE")
            pcon.commit()
            pcon.close()

            scur.execute("PRAGMA table_info("+table+")")
            cols = ""
            for c in scur: cols += c[1]+", "
            cols = cols[:-2] # remove last coma and space

            # update the initial revision 
            scur.execute("UPDATE initial_revision SET rev = "+str(max_rev)+" WHERE table_name = '"+table+"'")
            
            scur.execute("UPDATE "+table+" "+
                    "SET "+branch+"_rev_end = "+str(max_rev)+" "+
                    "WHERE "+branch+"_rev_end = "+str(rev))
            scur.execute("UPDATE "+table+" "+
                    "SET "+branch+"_rev_begin = "+str(max_rev+1)+" "+
                    "WHERE "+branch+"_rev_begin = "+str(rev+1))
            
            # we cannot add constrain to the spatialite db in order to have spatialite
            # update parent and child when we bump inserted hid above the max hid in the diff
            # we must do this manually
            scur.execute("SELECT MAX(OGC_FID) FROM "+table+"_diff")
            [max_pg_hid] = scur.fetchone()
            print "max pg hid", max_pg_hid
            scur.execute("SELECT MIN(OGC_FID) "+
                "FROM "+table+" "+
                "WHERE "+branch+"_rev_begin = "+str(max_rev+1))
            [min_sl_hid] = scur.fetchone()
            print "min sl hid", min_sl_hid
            if max_pg_hid and min_sl_hid: # one of them is empty, no conflict possible
                bump = max_pg_hid - min_sl_hid + 1
                if bump > 0:
                    # now bump the hids of inserted rows in working copy
                    scur.execute("UPDATE "+table+" "+
                            "SET OGC_FID = OGC_FID + "+str(bump)+" "+
                            "WHERE "+branch+"_rev_begin = "+str(max_rev+1))
                    # and bump the hid in the child field
                    # not that we don't care for nulls since adding something to null is null
                    scur.execute("UPDATE "+table+" "+
                            "SET "+branch+"_child = "+branch+"_child  + "+str(bump)+" "+
                            "WHERE "+branch+"_rev_end = "+str(max_rev))
                else:
                    print "our min added hid is superior to the max added hid in the posgtgis database"

                # detect conflicts: conflict occur if two lines with the same hid have
                # been modified (i.e. have a non null child) or one has been removed
                # and the other modified
                scur.execute("DROP VIEW  IF EXISTS "+table+"_conflicts_ogc_fid")
                sql=("CREATE VIEW "+table+"_conflicts_ogc_fid AS "+
                    "SELECT sl.OGC_FID as conflict_deleted_fid "+
                    "FROM "+table+" AS sl, "+table+"_diff AS pg "+
                    "WHERE sl.OGC_FID = pg.OGC_FID "+
                        "AND (sl."+branch+"_child != pg."+branch+"_child "+
                            "OR (sl."+branch+"_child IS NULL AND pg."+branch+"_child IS NOT NULL) "
                            "OR (sl."+branch+"_child IS NOT NULL AND pg."+branch+"_child IS NULL))")
                print sql
                scur.execute(sql)
                scur.execute("SELECT * FROM  "+table+"_conflicts_ogc_fid" )
                sl_pg = scur.fetchall()
                if sl_pg:
                    print "there are conflicts"
                    # add layer for conflicts
                    scur.execute("DROP TABLE IF EXISTS "+table+"_conflicts ")
                    sql=("CREATE TABLE "+table+"_conflicts AS "+
                        # insert new features from mine
                        "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, 'modified' AS action, "+cols+" FROM "+table+", "+table+"_conflicts_ogc_fid AS cflt "+
                        "WHERE OGC_FID = (SELECT "+branch+"_child FROM "+table+" "+
                                             "WHERE OGC_FID = conflict_deleted_fid) "+
                        "UNION ALL "
                        # insert new features from theirs
                        "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, 'modified' AS action, "+cols+" FROM "+table+"_diff "+", "+table+"_conflicts_ogc_fid AS cflt "+
                        "WHERE OGC_FID = (SELECT "+branch+"_child FROM "+table+"_diff "+
                                             "WHERE OGC_FID = conflict_deleted_fid) "+
                         # insert deleted features from mine
                        "UNION ALL "+
                        "SELECT "+branch+"_parent AS conflict_id, 'mine' AS origin, 'deleted' AS action, "+cols+" FROM "+table+", "+table+"_conflicts_ogc_fid AS cflt "+
                        "WHERE OGC_FID = conflict_deleted_fid AND "+branch+"_child IS NULL " +
                         # insert deleted features from theirs
                        "UNION ALL "+
                        "SELECT "+branch+"_parent AS conflict_id, 'theirs' AS origin, 'deleted' AS action, "+cols+" FROM "+table+"_diff, "+table+"_conflicts_ogc_fid AS cflt "+
                        "WHERE OGC_FID = conflict_deleted_fid AND "+branch+"_child IS NULL " )
                    print sql
                    scur.execute(sql)

                    # identify conflicts for deleted 
                    scur.execute("UPDATE "+table+"_conflicts "+
                            "SET conflict_id = OGC_FID "+
                            "WHERE action = 'deleted'")
                    scur.execute("DELETE FROM geometry_columns WHERE f_table_name = '"+table+"_conflicts'")
                    scur.execute("SELECT RecoverGeometryColumn('"+table+"_conflicts', 'GEOMETRY', (SELECT srid FROM geometry_columns WHERE f_table_name='"+table+"'), (SELECT type FROM geometry_columns WHERE f_table_name='"+table+"'), 'XY')")
                    
                    scur.execute("CREATE UNIQUE INDEX IF NOT EXISTS "+table+"_conflicts_idx ON "+table+"_conflicts(OGC_FID)")

                    # create trigers such that on delete the conflict is resolved
                    # for modified on both side:
                    #   - if we delete 'mine' 
                    #           - we delete the inserted on our side, 
                    #           - copy their inserted 
                    #           - and change our parent's child (replace by theirs)
                    #   - if we delete 'theirs' 
                    #           - we change the parent on our side to match theirs
                    #           - we then insert their child
                    #           - and set the end_revision and child of this record to point to our child
                    # for deleted on our side and modified on theirs:
                    #   - if we delete 'mine' 
                    #           - we just replace ours parent's child (delete) by theirs (modified) 
                    #           - and insert their child
                    #   - if we delete 'theirs' 
                    #           - we do the same thing, 
                    #           - plus we set the end revision on their child
                    # for modified on our side and deleted on theirs:
                    #   - if we delete 'mine' 
                    #           - we remove our child 
                    #           - and set the parent as theirs (deleted)
                    #   - if we delete 'theirs' 
                    #           - we set our parent as theirs (deleted without children)
                    #           - we update our child to set the parent to null
                    # in all case we end by deleting the 2 conflict rows in the conflict table
                    # and remove their parent and child from the diff

                    modified_on_both_sides_delete_mine = ("old.action = 'modified' AND old.origin = 'mine' "+
                        "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ) = 'modified'")

                    modified_on_both_sides_delete_theirs = ("old.action = 'modified' AND old.origin = 'theirs' "+
                        "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'mine' AND conflict_id = old.conflict_id ) = 'modified'")

                    deleted_on_our_sides_modified_on_theirs_delete_mine = ("old.action = 'deleted' AND old.origin = 'mine' "+
                        "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ) = 'modified'")

                    deleted_on_our_sides_modified_on_theirs_delete_theirs = ("old.action = 'modified' AND old.origin = 'theirs' "+
                        "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'mine' AND conflict_id = old.conflict_id ) = 'deleted'")

                    modified_on_our_sides_deleted_on_theirs_delete_mine = ("old.action = 'modified' AND old.origin = 'mine' "+
                        "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ) = 'deleted'")

                    modified_on_our_sides_deleted_on_theirs_delete_theirs = ("old.action = 'deleted' AND old.origin = 'theirs' "+
                        "AND (SELECT action FROM "+table+"_conflicts WHERE origin = 'mine' AND conflict_id = old.conflict_id ) = 'modified'")

                    scur.execute("DROP TRIGGER IF EXISTS delete_"+table+"_conflicts")
                    sql =("CREATE TRIGGER delete_"+table+"_conflicts AFTER DELETE ON "+table+"_conflicts\n"+
                        "BEGIN\n"+
                            "DELETE FROM "+table+" "+
                            "WHERE OGC_FID = old.OGC_FID AND "+modified_on_both_sides_delete_mine+";\n"+

                            "INSERT INTO "+table+"("+cols+") "+
                            "SELECT "+cols+" FROM "+table+"_diff "+
                            "WHERE OGC_FID = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ) "+
                                "AND "+modified_on_both_sides_delete_mine+";\n"+

                            "REPLACE INTO "+table+"("+cols+") "+
                            "SELECT "+cols+" FROM "+table+"_diff "+
                            "WHERE OGC_FID = old."+branch+"_parent AND "+modified_on_both_sides_delete_mine+";\n"+

                            "UPDATE "+table+" "+
                            "SET "+branch+"_child = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id) "+
                            "WHERE OGC_FID = old."+branch+"_parent "+
                                "AND "+modified_on_both_sides_delete_theirs+";\n"+

                            "INSERT INTO "+table+"("+cols+") "+
                            "SELECT "+cols+" FROM "+table+"_diff "+
                            "WHERE OGC_FID = old.OGC_FID "+
                                "AND "+modified_on_both_sides_delete_theirs+";\n"+

                            "UPDATE "+table+" "+
                            "SET "+branch+"_child = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'mine' AND conflict_id = old.conflict_id), "+
                                   branch+"_rev_end = "+str(max_rev)+" "+
                            "WHERE OGC_FID = old.OGC_FID "+
                                "AND "+modified_on_both_sides_delete_theirs+";\n"+

                            "REPLACE INTO "+table+"("+cols+") "+
                            "SELECT "+cols+" FROM "+table+"_diff "+
                            "WHERE OGC_FID = old."+branch+"_parent "+
                                "AND "+deleted_on_our_sides_modified_on_theirs_delete_mine+";\n"+

                            "INSERT INTO "+table+"("+cols+") "+
                            "SELECT "+cols+" FROM "+table+"_diff "+
                            "WHERE OGC_FID = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id) "+
                                "AND "+deleted_on_our_sides_modified_on_theirs_delete_mine+";\n"+

                            "REPLACE INTO "+table+"("+cols+") "+
                            "SELECT "+cols+" FROM "+table+"_diff "+
                            "WHERE OGC_FID = old."+branch+"_parent "+
                                "AND "+deleted_on_our_sides_modified_on_theirs_delete_theirs+";\n"+

                            "INSERT INTO "+table+"("+cols+") "+
                            "SELECT "+cols+" FROM "+table+"_diff "+
                            "WHERE OGC_FID = old.OGC_FID "+
                                "AND "+deleted_on_our_sides_modified_on_theirs_delete_theirs+";\n"+

                            "UPDATE "+table+" "+
                            "SET  "+branch+"_rev_end = "+str(max_rev)+" "+
                            "WHERE OGC_FID = old.OGC_FID "+
                                "AND "+deleted_on_our_sides_modified_on_theirs_delete_theirs+";\n"+

                            "DELETE FROM "+table+" "+
                            " WHERE OGC_FID = old.OGC_FID "+
                                "AND "+modified_on_our_sides_deleted_on_theirs_delete_mine+";\n"+

                            "REPLACE INTO "+table+"("+cols+") "+
                            "SELECT "+cols+" FROM "+table+"_diff "+
                            "WHERE OGC_FID = old."+branch+"_parent "+
                                "AND "+modified_on_our_sides_deleted_on_theirs_delete_mine+";\n"+

                            "REPLACE INTO "+table+"("+cols+") "+
                            "SELECT "+cols+" FROM "+table+"_diff "+
                            "WHERE OGC_FID = old."+branch+"_parent "+
                                "AND "+deleted_on_our_sides_modified_on_theirs_delete_theirs+";\n"+

                            "UPDATE "+table+" "
                            "SET "+branch+"_parent = NULL "+
                            "WHERE OGC_FID = old.OGC_FID "+
                                "AND "+deleted_on_our_sides_modified_on_theirs_delete_theirs+";\n"+

                            "DELETE FROM "+table+"_conflicts "+
                            "WHERE conflict_id = old.conflict_id;\n"+

                            "DELETE FROM "+table+"_diff "+
                            "WHERE (OGC_FID = (SELECT OGC_FID FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ) "+
                                    "OR OGC_FID = (SELECT "+branch+"_parent FROM "+table+"_conflicts WHERE origin = 'theirs' AND conflict_id = old.conflict_id ));\n"+
                        "END")
                    print sql
                    scur.execute(sql)

                    scon.commit()

                    self.iface.addVectorLayer("dbname="+uri.database()+" key=\"OGC_FID\" table=\""+table+"_conflicts\"(GEOMETRY)",table+"_conflicts",'spatialite')
                    conflict_for_this_layer = True

            scur.execute("CREATE UNIQUE INDEX IF NOT EXISTS "+table+"_diff_idx ON "+table+"_diff(OGC_FID)")
            if conflict_for_this_layer: 
                # insert inserted and modified and update deleted and modified that have no conflicts
                scur.execute("INSERT OR REPLACE INTO "+table+" ("+cols+") "+
                    "SELECT "+cols+" "+
                    "FROM "+table+"_diff "+
                        "LEFT JOIN (SELECT OGC_FID AS conflict_id FROM "+table+"_conflicts WHERE origin = 'theirs' "+
                             "UNION SELECT trunk_parent AS conflict_id FROM "+table+"_conflicts WHERE origin = 'theirs') AS c "+
                        "ON OGC_FID = c.conflict_id "+
                        "WHERE c.conflict_id IS NULL")
            else:
                # insert and replace all
                scur.execute("INSERT OR REPLACE INTO "+table+" ("+cols+") "+
                    "SELECT "+cols+" FROM "+table+"_diff")

            scon.commit()
            scon.close()

        if not self.unresolvedConflicts(): QMessageBox.information( self.iface.mainWindow(), "Notice", "Your are up to date with revision "+str(all_max_rev)+".")


    def checkout(self):
        """create working copy from versionned database layers"""
        filename = ""
        versionned_layers = {}
        for name,layer in QgsMapLayerRegistry.instance().mapLayers().iteritems():
            uri = QgsDataSourceURI(layer.source())
            if layer.providerType() == "postgres" and uri.schema()[-9:] == "_rev_head": 
                versionned_layers[name] = layer
        
        if not versionned_layers: 
            print "No versionned layer found"
            QMessageBox.information( self.iface.mainWindow(), "Notice", "No versionned layer found")
            return
        else:
            print "converting ", versionned_layers

        
        for name,layer in versionned_layers.iteritems():
            uri = QgsDataSourceURI(layer.source())

            schema = uri.schema()[:-9]
            table = uri.table()
            # remove _branch from name
            branch = schema[schema.rfind('_')+1:]
            schema = schema[:schema.rfind('_')]
            dbname = uri.database()

            # set the current and target rev for the local db
            con =  psycopg2.connect(uri.connectionInfo())
            cur = con.cursor()
            cur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
            current_rev = int(cur.fetchone()[0])
            rev = current_rev + 1
            print "next revision = ", rev
            assert(rev)
            con.close()

            # use ogr2ogr to create spatialite db
            if not filename:
                filename = QFileDialog.getSaveFileName(self.iface.mainWindow(), 'Save Versionned Layers As', '.', '*.sqlite')
                cmd = "ogr2ogr -preserve_fid -f SQLite -dsco SPATIALITE=yes "+filename+" PG:\""+uri.connectionInfo()+" active_schema="+schema+"\" "+table
                print cmd
                if os.path.isfile(filename): os.remove(filename)
                os.system(cmd)

                # save target revision in a table
                con = db.connect(filename)
                cur = con.cursor()
                #TODO: add the table in there such that we can have layer from multiples sources
                cur.execute("CREATE TABLE initial_revision AS SELECT "+
                        str(current_rev)+" AS rev, '"+
                        branch+"' AS branch, '"+
                        schema+"' AS table_schema, '"+
                        table+"' AS table_name, '"+
                        escapeQuotes(uri.connectionInfo())+"' AS conn_info")
                con.commit()
                con.close()
                
            else:
                cmd = "ogr2ogr -preserve_fid -f SQLite -update "+filename+" PG:\""+uri.connectionInfo()+" active_schema="+schema+"\" "+table
                print cmd
                os.system(cmd)

                # save target revision in a table if not in there
                con = db.connect(filename)
                cur = con.cursor()
                if not cur.fetchone(): # no record, insert
                    cur.execute("INSERT INTO initial_revision(rev, branch, table_schema, table_name, conn_info) VALUES ("+str(current_rev)+", '"+branch+"', '"+schema+"', '"+table+"', '"+escapeQuotes(uri.connectionInfo())+"')" )
                con.commit()
                con.close()

            con = db.connect(filename)
            cur = con.cursor()

            # create views and triggers in spatilite db
            cur.execute("PRAGMA table_info("+table+")")
            cols = ""
            newcols = ""
            hcols = {}
            for r in cur:
                if   r[1][-10:] == "_rev_begin" : hcols["rev_begin"] = r[1]
                elif r[1][-8:]  == "_rev_end" : hcols["rev_end"] = r[1]
                elif r[1][-6:]  == "_child" : hcols["child"] = r[1]
                elif r[1][-7:]  == "_parent" : hcols["parent"] = r[1]
                elif r[1]  == "OGC_FID" : pass
                else : 
                    cols += r[1] + ", "
                    newcols += "new."+r[1]+", "
            cols = cols[:-2]
            newcols = newcols[:-2] # remove last coma
            print cols
            print hcols
            con = db.connect(filename)
            cur = con.cursor()
            sql = "CREATE VIEW "+table+"_view "+"AS SELECT ROWID AS ROWID, OGC_FID, "+cols+" FROM "+table+" WHERE "+hcols['rev_end']+" IS NULL"
            print sql
            cur.execute(sql)

            cur.execute("DELETE FROM views_geometry_columns WHERE f_table_name = '"+table+"_conflicts'")
            sql = "INSERT INTO views_geometry_columns "+"(view_name, view_geometry, view_rowid, f_table_name, f_geometry_column) "+"VALUES"+"('"+table+"_view', 'GEOMETRY', 'ROWID', '"+table+"', 'GEOMETRY')"
            print sql 
            cur.execute(sql)  
             
            sql =("CREATE TRIGGER update_"+table+" INSTEAD OF UPDATE ON "+table+"_view\n"+
                  "BEGIN\n"+
                    "INSERT INTO "+table+" "
                    "(OGC_FID, "+cols+", "+hcols['rev_begin']+", "+hcols['parent']+") "+
                    "VALUES "
                    "((SELECT MAX(OGC_FID) + 1 FROM "+table+"), "+newcols+", (SELECT rev+1 FROM initial_revision WHERE table_name = '"+table+"'), old.OGC_FID);\n"+
                    "UPDATE "+table+" SET "+hcols['rev_end']+" = (SELECT rev FROM initial_revision WHERE table_name = '"+table+"'), "+hcols['child']+" = (SELECT MAX(OGC_FID) FROM "+table+") WHERE OGC_FID = old.OGC_FID;\n"+
                  "END")
            print sql
            cur.execute(sql)  

            sql =("CREATE TRIGGER insert_"+table+" INSTEAD OF INSERT ON "+table+"_view\n"+
                  "BEGIN\n"+
                    "INSERT INTO "+table+" "+ 
                    "(OGC_FID, "+cols+", "+hcols['rev_begin']+") "+
                    "VALUES "+
                    "((SELECT MAX(OGC_FID) + 1 FROM "+table+"), "+newcols+", (SELECT rev+1 FROM initial_revision WHERE table_name = '"+table+"'));\n"+
                  "END")
            print sql
            cur.execute(sql)  

            sql =("CREATE TRIGGER delete_"+table+" INSTEAD OF DELETE ON "+table+"_view\n"+
                  "BEGIN\n"+
                    "UPDATE "+table+" SET "+hcols['rev_end']+" = (SELECT rev FROM initial_revision WHERE table_name = '"+table+"') WHERE OGC_FID = old.OGC_FID;\n"+
                  "END")
            print sql 
            cur.execute(sql)

            
            con.commit()
            con.close()

            # replace layer by it's offline version
            QgsMapLayerRegistry.instance().removeMapLayer(name)
            self.iface.addVectorLayer("dbname="+filename+" key=\"OGC_FID\" table=\""+table+"_view\" (GEOMETRY)",table,'spatialite')


    def commit(self):
        """merge modifiactions into database"""
        print "commit"
        if self.unresolvedConflicts(): return

        versionned_layers = {}
        for name,layer in QgsMapLayerRegistry.instance().mapLayers().iteritems():
            uri = QgsDataSourceURI(layer.source())
            if layer.providerType() == "spatialite" and uri.table()[-5:] == "_view": 
                versionned_layers[name] = layer

        if not versionned_layers: 
            print "No versionned layer found"
            QMessageBox.information( self.iface.mainWindow(), "Notice", "No versionned layer found")
            return
        else:
            print "commiting ", versionned_layers

        for name,layer in versionned_layers.iteritems():
            uri = QgsDataSourceURI(layer.source())
            scon = db.connect(uri.database())
            scur = scon.cursor()
            table = uri.table()[:-5] # remove suffix _view
            scur.execute("SELECT rev, branch, table_schema, conn_info "+
                "FROM initial_revision "+
                "WHERE table_name = '"+table+"'")
            [rev, branch, table_schema, conn_info] = scur.fetchone()

            pcon = psycopg2.connect(conn_info)
            pcur = pcon.cursor()
            pcur.execute("SELECT MAX(rev) FROM "+table_schema+".revisions WHERE branch = '"+branch+"'")
            [max_rev] = pcur.fetchone()
            if max_rev != rev: 
                QMessageBox.warning(self.iface.mainWindow(), "Warning", "The table '"+table+"' in working copy '"+uri.database()+"' is not up to date (late by "+str(max_rev-rev)+" commit(s)).\n\nPlease update before commiting your modifications")
                print "aborted"
                return

        # time to get the commit message
        if not self.qCommitMsgDialog.exec_(): return
        commit_msg = self.commitMsgDialog.commitMessage.document().toPlainText()

        schema_list={} # for final cleanup
        nb_of_updated_layer = 0
        for name,layer in versionned_layers.iteritems():
            uri = QgsDataSourceURI(layer.source())
            scon = db.connect(uri.database())
            scur = scon.cursor()
            table = uri.table()[:-5] # remove suffix _view
            scur.execute("SELECT rev, branch, table_schema, conn_info  "+
                "FROM initial_revision "+
                "WHERE table_name = '"+table+"'")
            [rev, branch, table_schema, conn_info] = scur.fetchone()

            diff_schema = table_schema+"_"+branch+"_"+str(rev)+"_to_"+str(rev+1)+"_diff"

            scur.execute( "DROP TABLE IF EXISTS "+table+"_diff")

            # note, creating the diff table dirrectly with CREATE TABLE... AS SELECT won't work
            # types get fubared in the process
            # therefore we copy the creation statement from spatialite master and change the
            # table name ta obtain a similar table, we add the geometry column 
            # to geometry_columns manually and we insert the diffs
            scur.execute("SELECT sql FROM sqlite_master WHERE tbl_name = '"+table+"' AND type = 'table'")
            [sql] = scur.fetchone()
            sql = unicode.replace(sql,table,table+"_diff",1)
            scur.execute(sql)
            scur.execute("DELETE FROM geometry_columns WHERE f_table_name = '"+table+"_diff'")
            scur.execute("INSERT INTO geometry_columns SELECT '"+table+"_diff', 'GEOMETRY', type, coord_dimension, srid, spatial_index_enabled FROM geometry_columns WHERE f_table_name = '"+table+"'")
            scur.execute( "INSERT INTO "+table+"_diff "+
                    "SELECT * "+
                    "FROM "+table+" "+
                    "WHERE "+branch+"_rev_end = "+str(rev)+" OR "+branch+"_rev_begin > "+str(rev))
            scur.execute( "SELECT OGC_FID FROM "+table+"_diff")
            there_is_something_to_commit = scur.fetchone()
            print "there_is_something_to_commit ", there_is_something_to_commit
            scon.commit()

            pcon = psycopg2.connect(conn_info)
            pcur = pcon.cursor()

            # import layers in postgis schema
            pcur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '"+diff_schema+"'")
            res = pcur.fetchone()
            print "found schema",res
            if not res:
                schema_list[diff_schema] = conn_info
                print "creating schema ", diff_schema
                pcur.execute("CREATE SCHEMA "+diff_schema)
            pcur.execute( "DROP TABLE IF EXISTS "+diff_schema+"."+table+"_diff")
            pcon.commit()
            cmd = "ogr2ogr -preserve_fid -f PostgreSQL PG:\""+conn_info+" active_schema="+diff_schema+"\" -lco GEOMETRY_NAME=geom -lco FID=hid "+uri.database()+" "+table+"_diff"
            print cmd
            os.system(cmd)

            # remove dif table and geometry column
            scur.execute("DELETE FROM geometry_columns WHERE f_table_name = '"+table+"_diff'")
            scur.execute("DROP TABLE "+table+"_diff")

            if not there_is_something_to_commit: 
                print "nothing to commit for ", table
                pcon.close()
                continue

            nb_of_updated_layer += 1

            pcur.execute("SELECT rev FROM "+table_schema+".revisions WHERE rev = "+str(rev+1))
            if not pcur.fetchone():
                print "inserting rev ", str(rev+1)
                pcur.execute("INSERT INTO "+table_schema+".revisions (rev, commit_msg, branch, author) VALUES ("+str(rev+1)+", '"+commit_msg+"', '"+branch+"', 'dummy')")

            # add  constrain  such that we can
            # update the new hid and have posgres update the child hid fields accordingly
            # since those fields where updated through views, its pretty sure it will work
            pcur.execute("ALTER TABLE "+diff_schema+"."+table+"_diff "+
                "ADD CONSTRAINT "+table+"_"+branch+"__child_fkey "+
                "FOREIGN KEY("+branch+"_child) "+
                "REFERENCES "+diff_schema+"."+table+"_diff (hid) ON UPDATE CASCADE")
            # now we bump all hids for insertions
            pcur.execute("WITH "+
                     "max_hid AS (SELECT MAX(hid) AS hid "+
                                 "FROM "+table_schema+"."+table+"), "+
                     "hids AS (SELECT old.hid AS old, "+
                                    "max_hid.hid+(row_number() OVER())::integer AS new "+
                                  "FROM  "+diff_schema+"."+table+"_diff AS old, max_hid "+
                                  "WHERE "+branch+"_rev_begin = "+str(rev+1)+") "+
                "UPDATE "+diff_schema+"."+table+"_diff AS src "+
                "SET hid = hids.new "+ 
                "FROM hids "+
                "WHERE src.hid = hids.old") 

            pcur.execute("SELECT column_name "+
                    "FROM information_schema.columns "+
                    "WHERE table_schema = '"+table_schema+"' AND table_name = '"+table+"'")
            cols = ""
            for c in pcur: 
                if c[0] != "hid": cols += c[0]+", "
            cols = cols[:-2] # remove last coma and space
            # insert inserted and modified
            pcur.execute("INSERT INTO "+table_schema+"."+table+" ("+cols+") "+
                "SELECT "+cols+" FROM "+diff_schema+"."+table+"_diff "+
                "WHERE "+branch+"_rev_begin = "+str(rev+1))

            # update deleted and modified 
            pcur.execute("UPDATE "+table_schema+"."+table+" AS dest "+
                    "SET ("+branch+"_rev_end, "+branch+"_child)=(src."+branch+"_rev_end, src."+branch+"_child) "+
                    "FROM "+diff_schema+"."+table+"_diff AS src "+
                    "WHERE dest.hid = src.hid AND src."""+branch+"_rev_end = "+str(rev))
            pcon.commit()
            pcon.close()

        if nb_of_updated_layer: 
            for name,layer in versionned_layers.iteritems():
                uri = QgsDataSourceURI(layer.source())
                scon = db.connect(uri.database())
                scur = scon.cursor()
                table = uri.table()[:-5] # remove suffix _view
                scur.execute("UPDATE initial_revision SET rev = rev+1 WHERE table_name = '"+table+"'")
                scon.commit()
                scon.close()
            QMessageBox.information(self.iface.mainWindow(), "Info", "You have successfully commited revision "+str(rev+1))
        else:
            QMessageBox.information(self.iface.mainWindow(), "Info", "There was no modification to commit")


        for schema, conn_info in schema_list.iteritems(): 
            pcon = psycopg2.connect(conn_info)
            pcur = pcon.cursor()
            pcur.execute("DROP SCHEMA "+schema+" CASCADE")
            pcon.commit()
            pcon.close()

    def versionnedSchemas(self, conn_name):
        self.connectionDialog.comboBoxSchema.clear()
        print conn_name
        con = self.pgConnect(conn_name)
        cur = con.cursor()
        cur.execute("SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_name = 'revisions'")
        for r in cur: self.connectionDialog.comboBoxSchema.addItem(r[0])
        con.close()

    def pgConnect(self, conn_name):
        settings = QSettings()
        return  psycopg2.connect(
                database = settings.value("PostgreSQL/connections/"+conn_name+"/database"),
                user     = settings.value("PostgreSQL/connections/"+conn_name+"/user"),
                host     = settings.value("PostgreSQL/connections/"+conn_name+"/host"),
                password = settings.value("PostgreSQL/connections/"+conn_name+"/password"),
                port     = settings.value("PostgreSQL/connections/"+conn_name+"/port")
                )

    def pgCreateSchema(self, conn_name, schema):
        con = pgConnect(conn_name)
        cur = con.cursor()
        try:
            cur.execute("CREATE SCHEMA "+schema)
            con.commit()
        except:
            ans =  QMessageBox.warning(self.iface.mainWindow(), "Warning", "The schema '"+schema+"'  already exists (maybe from an abborted operation).\n\nDo you want to delete it an proceed ?", QMessageBox.Yes | QMessageBox.No)
            if ans == QMessageBox.Yes:
                con.commit()
                cur.execute("DROP SCHEMA "+schema+" CASCADE")
                cur.execute("CREATE SCHEMA "+schema)
            else:
                print "aborted"
                con.close()
                return False
        return True

    def getConnections(self):
        # get list of connections
        settings = QSettings()
        settings.beginGroup("PostgreSQL/connections")
        pgGonnections=[settings.childGroups()]
        settings.endGroup()
        selectedConnection = settings.value("PostgreSQL/connections/selected")

        # find all versionned spatilite layers
        registry = QgsMapLayerRegistry.instance()
        local_files = []
        for name,layer in registry.mapLayers().iteritems():
            uri = QgsDataSourceURI(layer.source())
            if layer.providerType() == "spatialite" and uri.table()[-5:] == "_view": 
                local_files.append( uri.database() )
        if not local_files: 
            print "No versionned layer found"
            QMessageBox.information( self.iface.mainWindow(), "Notice", "No versionned layer found")
            return
        else:
            print "converting ", local_files

        # setup connectionDialog
        self.connectionDialog.comboBoxLocalDb.clear()
        self.connectionDialog.comboBoxLocalDb.addItems(local_files)

        self.connectionDialog.comboBoxConnection.clear()
        self.connectionDialog.comboBoxConnection.addItems(pgGonnections[0])

        if selectedConnection:
            self.connectionDialog.comboBoxConnection.setCurrentIndex( self.connectionDialog.comboBoxConnection.findText(selectedConnection) )
            self.versionnedSchemas(selectedConnection)
        
        QObject.connect(self.connectionDialog.comboBoxConnection, SIGNAL("currentIndexChanged(const QString &)"), self.versionnedSchemas)

        ok = self.qConnectionDialog.exec_()
        QObject.disconnect(self.connectionDialog.comboBoxConnection, SIGNAL("currentIndexChanged(const QString &)"), self.versionnedSchemas)
        return ok

