
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

qset = QSettings( "oslandia", "horao_qgis_plugin" )

WIN_TITLE = "versioning"

class Versioning:
    def __init__(self, iface):
        # Save reference to the QGIS interface
        self.iface = iface
        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)

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
        for name,layer in registry.mapLayers().iteritems():
            if layer.providerType() == "postgres":
                uri = QgsDataSourceURI(layer.source())
                if uri.schema()[-9:] == "_rev_head":
                    schema = uri.schema()[:-9]
                    table = uri.table()
                    # remove _branch from name
                    branch = schema[schema.rfind('_'):]
                    schema = schema[:schema.rfind('_')]
                    dbname = uri.database()
                    # use ogr2ogr to create spatialite db
                    if not filename:
                        filename = QFileDialog.getSaveFileName(self.iface.mainWindow(), 'Save Versionned Layers As', '.', '*.sqlite')
                        cmd = "ogr2ogr -preserve_fid -f SQLite -dsco SPATIALITE=yes "+filename+" PG:\"dbname='"+dbname+"' active_schema="+schema+"\" "+table
                        print cmd
                        if os.path.isfile(filename): os.remove(filename)
                        os.system(cmd)


                    else:
                        cmd = "ogr2ogr -preserve_fid -f SQLite -update "+filename+" PG:\"dbname='"+dbname+"' active_schema="+schema+"\" "+table
                        print cmd
                        os.system(cmd)

                    con =  psycopg2.connect(database=dbname)
                    cur = con.cursor()
                    cur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
                    rev = int(cur.fetchone()[0]) + 1
                    print "next revision = ", rev
                    assert(rev)
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



        
        pass

    def commit(self):
        print "Versioning.commit"
        pass
