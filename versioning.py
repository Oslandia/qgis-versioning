
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
from commit_ui import *

qset = QSettings( "oslandia", "horao_qgis_plugin" )

WIN_TITLE = "versioning"

class Versioning:
    def __init__(self, iface):
        # Save reference to the QGIS interface
        self.iface = iface
        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)

        self.qdialog = QDialog(self.iface.mainWindow())
        self.dialog = Ui_Dialog()
        self.dialog.setupUi(self.qdialog)

    def initGui(self):
        # Create action  work_offline
        self.work_offline_action = QAction(
            QIcon(":/plugins/versioning/work_offline.svg"),
            u"work offline", self.iface.mainWindow())
        self.work_offline_action.setWhatsThis("work offline")
        # connect the action to the run method
        self.work_offline_action.triggered.connect(self.work_offline)

        # Add toolbar button and menu item
        self.iface.addToolBarIcon(self.work_offline_action)
        self.iface.addPluginToMenu( WIN_TITLE, self.work_offline_action)

        # Create action commit
        self.commit_action = QAction(
            QIcon(":/plugins/versioning/commit.svg"),
            u"commit", self.iface.mainWindow())
        # connect the action to the run method
        self.commit_action.triggered.connect(self.commit)

        # Add toolbar button and menu item
        self.iface.addToolBarIcon(self.commit_action)
        self.iface.addPluginToMenu( WIN_TITLE, self.commit_action)

    def unload(self):
        # Remove the plugin menu item and icon
        self.iface.removePluginMenu( WIN_TITLE, self.work_offline_action)
        self.iface.removePluginMenu( WIN_TITLE, self.commit_action)
        self.iface.removeToolBarIcon(self.work_offline_action)
        self.iface.removeToolBarIcon(self.commit_action)

    def work_offline(self):
        registry = QgsMapLayerRegistry.instance()
        filename = ""
        versionned_layers = {}
        for name,layer in registry.mapLayers().iteritems():
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
            con =  psycopg2.connect(database=dbname)
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
                cmd = "ogr2ogr -preserve_fid -f SQLite -dsco SPATIALITE=yes "+filename+" PG:\"dbname='"+dbname+"' active_schema="+schema+"\" "+table
                print cmd
                if os.path.isfile(filename): os.remove(filename)
                os.system(cmd)

                # save target revision in a table
                con = db.connect(filename)
                cur = con.cursor()
                cur.execute("CREATE TABLE initial_revision AS SELECT "+str(current_rev)+" AS rev, '"+branch+"' AS branch, '"+schema+"' AS schema" )
                con.commit()
                con.close()
                
            else:
                cmd = "ogr2ogr -preserve_fid -f SQLite -update "+filename+" PG:\"dbname='"+dbname+"' active_schema="+schema+"\" "+table
                print cmd
                os.system(cmd)

                # save target revision in a table if not in there
                con = db.connect(filename)
                cur = con.cursor()
                cur.execute("SELECT rev, branch, schema FROM initial_revision WHERE rev == "+str(current_rev)+" AND branch = '"+branch+"' AND schema = '"+schema+"'" )
                if not cur.fetchone(): # no record, insert
                    cur.execute("INSERT INTO TABLE initial_revision VALUES ("+str(current_rev)+", '"+branch+"', '"+schema+"')" )
                con.commit()
                con.close()

            # create views and triggers in spatilite db
            con = db.connect(filename)
            cur = con.cursor()
            cur.execute("PRAGMA table_info("+table+")")
            cols = ""
            newcols = ""
            hcols = {}
            for r in cur:
                if   r[1][-10:] == "_rev_begin" : hcols["rev_begin"] = r[1]
                elif r[1][-8:]  == "_rev_end" : hcols["rev_end"] = r[1]
                elif r[1][-6:]  == "_child" : hcols["child"] = r[1]
                elif r[1][-7:]  == "_parent" : hcols["parent"] = r[1]
                else : 
                    cols += r[1] + ", "
                    newcols += "new."+r[1]+", "
            cols = cols[:-2]
            newcols = newcols[13:-2] # remove last coma, and remove new.OGC_FID
            print cols
            print hcols
            con = db.connect(filename)
            cur = con.cursor()
            sql = "CREATE VIEW "+table+"_view "+"AS SELECT ROWID AS ROWID, "+cols+" FROM "+table+" WHERE "+hcols['rev_end']+" IS NULL"
            print sql
            cur.execute(sql)

            sql = "INSERT INTO views_geometry_columns "+"(view_name, view_geometry, view_rowid, f_table_name, f_geometry_column) "+"VALUES"+"('"+table+"_view', 'GEOMETRY', 'ROWID', '"+table+"', 'GEOMETRY')"
            print sql 
            cur.execute(sql)  
             
            sql = """
                CREATE TRIGGER update_"""+table+""" INSTEAD OF UPDATE ON """+table+"""_view
                  BEGIN
                    INSERT INTO """+table+"""
                    ("""+cols+""", """+hcols['rev_begin']+""", """+hcols['parent']+""")
                    VALUES
                    ((SELECT MAX(OGC_FID) + 1 FROM """+table+"""), """+newcols+""", """+str(rev)+""", old.OGC_FID);
                    UPDATE """+table+""" SET trunk_rev_end = """+str(rev-1)+""", """+hcols['child']+""" = (SELECT MAX(OGC_FID) FROM """+table+""") WHERE OGC_FID = old.OGC_FID;
                  END
                  """
            print sql
            cur.execute(sql)  

            sql = """
                CREATE TRIGGER insert_"""+table+""" INSTEAD OF INSERT ON """+table+"""_view
                  BEGIN
                    INSERT INTO """+table+""" 
                    ("""+cols+""", """+hcols['rev_begin']+""")
                    VALUES
                    ((SELECT MAX(OGC_FID) + 1 FROM """+table+"""), """+newcols+""", """+str(rev)+""");
                  END
                  """
            print sql
            cur.execute(sql)  

            sql = """
                CREATE TRIGGER delete_"""+table+""" INSTEAD OF DELETE ON """+table+"""_view
                  BEGIN
                    UPDATE """+table+""" SET """+hcols['rev_end']+""" = """+str(rev-1)+""" WHERE OGC_FID = old.OGC_FID;
                  END;
                  """
            print sql 
            cur.execute(sql)

            
            con.commit()
            con.close()

            # replace layer by it's offline version
            registry.removeMapLayer(name)
            self.iface.addVectorLayer("dbname="+filename+" key=\"OGC_FID\" table=\""+table+"_view\" (GEOMETRY)",table,'spatialite')

    def commit(self):
        print "Versioning.commit"
        # get list of connections
        settings = QSettings()
        settings.beginGroup("PostgreSQL/connections")
        pgGonnections=[settings.childGroups()]
        settings.endGroup()
        selectedConnection = settings.value("PostgreSQL/connections/selected")
        print selectedConnection

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

        # setup dialog
        self.dialog.comboBoxLocalDb.clear()
        self.dialog.comboBoxLocalDb.addItems(local_files)

        self.dialog.comboBoxConnection.clear()
        self.dialog.comboBoxConnection.addItems(pgGonnections[0])

        if selectedConnection:
            self.dialog.comboBoxConnection.setCurrentIndex( self.dialog.comboBoxConnection.findText(selectedConnection) )
            self.versionnedSchemas(selectedConnection)
        
        QObject.connect(self.dialog.comboBoxConnection, SIGNAL("currentIndexChanged(const QString &)"), self.versionnedSchemas)

        ok = self.qdialog.exec_()
        QObject.disconnect(self.dialog.comboBoxConnection, SIGNAL("currentIndexChanged(const QString &)"), self.versionnedSchemas)
        if not ok: return

        dbname = self.dialog.comboBoxConnection.currentText()
        schema = self.dialog.comboBoxSchema.currentText()
        local_file = self.dialog.comboBoxLocalDb.currentText()
        commit_msg = self.dialog.commitMessage.document().toPlainText()

        con = db.connect(local_file)
        cur = con.cursor()
        cur.execute("SELECT rev, branch FROM initial_revision WHERE schema = '"+schema+"'")
        target_rev, branch = cur.fetchone()
        assert(target_rev)
        target_rev = target_rev + 1
        con.commit()
        con.close()

        # check if a commit occured between chechout and now
        con =  psycopg2.connect(database=dbname)
        cur = con.cursor()
        sql = "SELECT dest.rev - "+str(target_rev)+"  FROM (SELECT MAX(rev) AS rev FROM "+schema+".revisions) AS dest;"
        cur.execute(sql)
        nb_of_commit_since_checkout = cur.fetchone()[0] + 1
        assert(nb_of_commit_since_checkout>=0)
        
        if nb_of_commit_since_checkout:
            QMessageBox.warning(self.iface.mainWindow(), "Warning", "There has been "+str(nb_of_commit_since_checkout)+" commit(s) since checkout.\n\n The update function is not yet supported, sorry!")
            print "abborted"
            return # TODO this part will be in the update function

            #for table in tables:
            #    # add the constrains lost in translation (postgis->spatilite) such that we can
            #    # update the new hid and have posgres update the child hid fields accordingly
            #    # since those field where updated through views, its pretty sure it will work
            #    t = commit_schema+"."+tables
            #    sql = "ALTER TABLE "+t+" ADD CONSTRAINT "+table+"_"+hcols['child']+"_fkey FOREIGN KEY("+hcols['child']+") REFERENCES "+t+i"(hid) ON UPDATE CASCADE;"
            #    cur.execute(sql)


        # create convert back to posgres to do the merging
        assert(dbname and schema)
        user = settings.value("PostgreSQL/connections/"+dbname+"/username")
        commit_schema = schema+"_"+user+"_commit"
        try:
            cur.execute("CREATE SCHEMA "+commit_schema)
        except:
            ans =  QMessageBox.warning(self.iface.mainWindow(), "Warning", "An unfinished commit from '"+user+"' on '"+schema+"' already exists (maybe from an abborted commit).\n\nDo you want to delete it an proceed ?", QMessageBox.Yes | QMessageBox.No)
            if ans == QMessageBox.Yes:
                con.commit()
                cur.execute("DROP SCHEMA "+commit_schema+" CASCADE")
                cur.execute("CREATE SCHEMA "+commit_schema)
            else:
                print "aborted"
                con.close()
                return
        con.commit()


        # import layers in postgis schema
        cmd = "ogr2ogr -f PostgreSQL PG:\"dbname='"+dbname+"' active_schema="+commit_schema+" user='"+user+"' \" -lco GEOMETRY_NAME=geom -lco FID=hid "+local_file
        os.system(cmd)

        # get all tables
        sql = "SELECT f_table_name from geometry_columns where f_table_schema='"+commit_schema+"' AND f_table_name NOT LIKE '%_view'"
        cur.execute(sql)
        tables = []
        for r in cur: tables.append(r[0])
        
        print "tabless to update:", tables
            
        sql="INSERT INTO "+schema+".revisions VALUES ("+str(target_rev)+",'"+commit_msg+"','"+branch+"');"
        cur.execute(sql)

        for table in tables:
            # fetch column names, except for history columns
            sql= """
                 SELECT column_name
                 FROM information_schema.columns
                 WHERE table_schema = '"""+schema+"""'
                 AND table_name = '"""+table+"""'
                 AND column_name NOT LIKE '%_rev_begin'
                 AND column_name NOT LIKE '%_rev_end'
                 AND column_name NOT LIKE '%_parent'
                 AND column_name NOT LIKE '%_child'
                 """
            cur.execute(sql)
            cols = ""
            for c in cur: cols += c[0]+", "
            cols = cols[:-2] # remove last ', '
            branch_rev_begin = branch+"_rev_begin"
            branch_rev_end = branch+"_rev_end"
            branch_parent = branch+"_parent"
            branch_child = branch+"_child"

            src_tbl = commit_schema+"."+table
            dst_tbl = schema+"."+table
        
            # insert inserted and modified
            sql = """
                INSERT INTO """+dst_tbl+"("+cols+", "+branch_rev_begin+", "+branch_parent+""")
                    SELECT """+cols+", "+branch_rev_begin+", "+branch_parent+"""
                    FROM """+src_tbl+""" 
                    WHERE """+branch_rev_begin+" = "+str(target_rev)
            print sql
            cur.execute(sql)

            # update deleted and modified 
            sql = """
                UPDATE """+dst_tbl+""" AS dest
                SET ("""+branch_rev_end+", "+branch_child+")=(src."+branch_rev_end+", src."+branch_child+""")
                FROM """+src_tbl+""" AS src
                WHERE dest.hid = src.hid AND src."""+branch_rev_end+" = "+str(target_rev-1);
            print sql
            cur.execute(sql)
        con.commit()
        # we can get rid of the commit schema
        sql="DROP SCHEMA "+commit_schema+" CASCADE"
        print sql
        cur.execute(sql)
        con.commit()
        con.close()
        QMessageBox.information(self.iface.mainWindow(), "Info", "You have successfully commited revision "+str(target_rev))

    def versionnedSchemas(self, dbname):
        self.dialog.comboBoxSchema.clear()
        con = psycopg2.connect(database=dbname)
        cur = con.cursor()
        cur.execute("SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_name = 'revisions'")
        for r in cur: self.dialog.comboBoxSchema.addItem(r[0])
        con.close()



