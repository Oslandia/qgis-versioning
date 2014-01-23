
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
import versioning_base

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

    def versionnedLayers(self):
        """Return a map of versionned layers with theys name as key()
        versionned layer are spatialite layers pointing to a table 
        with a name ending with _view """
        versionned_layers = {}
        for name,layer in QgsMapLayerRegistry.instance().mapLayers().iteritems():
            uri = QgsDataSourceURI(layer.source())
            if layer.providerType() == "spatialite" and uri.table()[-5:] == "_view": 
                versionned_layers[name] = layer
        return versionned_layers

    def sqliteFilenames(self):
        """Returns a list of sqlite filenames for all versionned layers"""
        sqlite_filenames = set();
        for name, layer in self.versionnedLayers().iteritems():
            uri = QgsDataSourceURI(layer.source())
            sqlite_filenames.add( uri.database() );
        return list(sqlite_filenames)

    def unresolvedConflicts(self):
        found = []
        for f in self.sqliteFilenames(): 
            unresolved = versioning_base.unresolvedConflicts( f )
            found.extend( unresolved )
            for c in unresolved:
                table = c+"_conflicts"
                if not QgsMapLayerRegistry.instance().mapLayersByName(table):
                    self.iface.addVectorLayer("dbname="+f+" key=\"OGC_FID\" table=\""+table+"\"(GEOMETRY)",table,'spatialite')

        if found: 
            QMessageBox.warning( self.iface.mainWindow(), "Warning", "Unresolved conflics for layer(s) "+', '.join(found)+".\n\nPlease resolve conflicts by openning the conflict layer atribute table and deleting either 'mine' or 'theirs' before continuing.\n\nPlease note that the attribute table is not refreshed on save (known bug), once you have deleted the unwanted change in the conflict layer, close and reopen the attribute table to check it's empty.")
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
        if not self.versionnedLayers(): 
            print "No versionned layer found"
            QMessageBox.information( self.iface.mainWindow(), "Notice", "No versionned layer found")
            return
        else:
            print "updating ", self.versionnedLayers()

        up_to_date = ""
        for f in self.sqliteFilenames(): 
            rev = versioning_base.update( f )
            up_to_date = up_to_date + f + " at revision "+str(versioning_base.revision(f))+", "

        if not self.unresolvedConflicts(): QMessageBox.information( self.iface.mainWindow(), "Notice", "Your are up to date with "+up_to_date[:-2]+".")



    def checkout(self):
        """create working copy from versionned database layers"""
        pg_versionned_layers = {}
        for name,layer in QgsMapLayerRegistry.instance().mapLayers().iteritems():
            uri = QgsDataSourceURI(layer.source())
            if layer.providerType() == "postgres" and uri.schema()[-9:] == "_rev_head": 
                pg_versionned_layers[name] = layer
        
        if not pg_versionned_layers: 
            print "No versionned layer found"
            QMessageBox.information( self.iface.mainWindow(), "Notice", "No versionned layer found")
            return
        else:
            print "converting ", pg_versionned_layers

        # for each connection, we need the list of tables
        tables_for_conninfo = {}
        for name,layer in pg_versionned_layers.iteritems():
            uri = QgsDataSourceURI(layer.source())
            conn_info = uri.connectionInfo()
            table =  uri.schema()+"."+uri.table()
            if conn_info in tables_for_conninfo: 
                tables_for_conninfo[conn_info].add(table)
            else: 
                tables_for_conninfo[conn_info] = set([table])

        filename = QFileDialog.getSaveFileName(self.iface.mainWindow(), 'Save Versionned Layers As', '.', '*.sqlite')

        if os.path.isfile(filename): os.remove(filename)

        for conn_info, tables in tables_for_conninfo.iteritems():
            print "checkin out ", tables, " from ", conn_info
            versioning_base.checkout( conn_info, list(tables), filename )

        
        # replace layers by their offline version
        for name,layer in pg_versionned_layers.iteritems():
            uri = QgsDataSourceURI(layer.source())
            table = uri.table()
            display_name = layer.name()
            print "replacing ", display_name
            QgsMapLayerRegistry.instance().removeMapLayer(name)
            self.iface.addVectorLayer("dbname="+filename+" key=\"OGC_FID\" table=\""+table+"_view\" (GEOMETRY)",display_name,'spatialite')


    def commit(self):
        """merge modifiactions into database"""
        print "commit"
        if self.unresolvedConflicts(): return

        if not self.versionnedLayers(): 
            print "No versionned layer found"
            QMessageBox.information( self.iface.mainWindow(), "Notice", "No versionned layer found")
            return
        else:
            print "commiting ", self.versionnedLayers()

        for f in self.sqliteFilenames():
            lateBy = versioning_base.late( f )
            if lateBy: 
                QMessageBox.warning(self.iface.mainWindow(), "Warning", "The working copy in "+f+" is not up to date (late by "+str(lateBy)+" commit(s)).\n\nPlease update before commiting your modifications")
                print "aborted"
                return

        # time to get the commit message
        if not self.qCommitMsgDialog.exec_(): return
        commit_msg = self.commitMsgDialog.commitMessage.document().toPlainText()
        if not commit_msg:
            QMessageBox.warning(self.iface.mainWindow(), "Warning", "No commit message, aborting commit")
            print "aborted"
            return

        for f in self.sqliteFilenames():
            nb_of_updated_layer = versioning_base.commit( f, commit_msg )
            if nb_of_updated_layer:

                QMessageBox.information(self.iface.mainWindow(), "Info", "You have successfully commited revision "+str( versioning_base.revision(f) ) )
            else:
                QMessageBox.information(self.iface.mainWindow(), "Info", "There was no modification to commit")


