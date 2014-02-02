
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
import re
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

# We start from layers comming from one or more postgis non-versionned schemata
# A widget group is displayed for each distinct schema (identified with 'dbname schema')
# The widget goup contains a branch and version combobox extracted from layers
# You can only chechout head revision
# If you select a new branch, you have to enter the name and it will be created from either the current working copy or the current branch/rev
# If you select a revision, the corresponding view in the db will be created and the layers replaced

# The list of postgres connections can be found either in layers, or in working copy
# The list of working copies can be found either in layers or in filesystem

# BRANCHES have no underscore, no spaces

class Versioning:

    def __init__(self, iface):
        # Save reference to the QGIS interface
        self.iface = iface
        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)

        self.qCommitMsgDialog = QDialog(self.iface.mainWindow())
        self.commitMsgDialog =commit_msg_ui.Ui_CommitMsgDialog()
        self.commitMsgDialog.setupUi(self.qCommitMsgDialog)

        self.currentLayers = []
        self.actions = []

    def onLegendClick(self, current, column=0):
        name = ''
        self.currentLayers = []
        self.info.setText('No group selected')
        for a in self.actions:
            if   a.text() == 'checkout' : a.setVisible(False)
            elif a.text() == 'update'   : a.setVisible(False)
            elif a.text() == 'commit'   : a.setVisible(False)
            elif a.text() == 'view'     : a.setVisible(False)
            elif a.text() == 'branch'   : a.setVisible(False)
        if current: 
            name = current.text(0)
        # we could look if we have something in selected layers
        # but we prefer impose grouping, otherwize it'll be easy to make errors

        # need to get all layers including subgroups
        rel = self.iface.legendInterface().groupLayerRelationship()
        relMap = {}
        for g,l in rel: relMap[g] = l

        if name not in relMap: # not a group
            return
        
        replaced = True
        while replaced:
            replaced = False
            for i,item in enumerate(relMap[name]):
                if item in relMap: 
                    relMap[name][i:i+1] = relMap[item]
                    replaced = True

        self.currentLayers = relMap[name]
        # we should check that the selection is homogeneous
        previous_conn = ()
        for layerId in self.currentLayers:
            layer = QgsMapLayerRegistry.instance().mapLayer( layerId )
            uri = QgsDataSourceURI(layer.source())
            if previous_conn:
                if (uri.database(), uri.schema()) != previous_conn:
                    currentLayers = []
                    self.info.setText("Selected group doesn't share the same database and/or schema")
                    return
            else:
                previous_conn = (uri.database(), uri.schema())

        if not self.currentLayers: return

        layer = QgsMapLayerRegistry.instance().mapLayer( self.currentLayers[0] )
        uri = QgsDataSourceURI( layer.source() )
        selectionType = ''
        if layer.providerType() == "spatialite": 
            rev = 0
            try: 
                rev = versioning_base.revision( uri.database() )
            except:
                currentLayers = []
                self.info.setText("The selected group is not a working copy")
                return
            self.info.setText( uri.database() +' rev='+str(rev))
            selectionType = 'working copy'
        if layer.providerType() == "postgres": 
            m = re.match('(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
            if m: 
                self.info.setText(uri.database()+' '+m.group(1)+' branch='+m.group(2)+' rev='+m.group(3))
                if m.group(3) == 'head': selectionType = 'head'
                else: selectionType = 'versioned'
            else:
                selectionType = 'unversioned'
        
        # refresh the available commands
        assert( selectionType )
        if selectionType == 'unversioned':
            for a in self.actions:
                pass
        elif selectionType == 'versioned':
            for a in self.actions:
                if   a.text() == 'view'     : a.setVisible(True)
                elif a.text() == 'branch'   : a.setVisible(True)
        elif selectionType == 'head':
            for a in self.actions:
                if   a.text() == 'checkout' : a.setVisible(True)
                elif a.text() == 'view'     : a.setVisible(True)
                elif a.text() == 'branch'   : a.setVisible(True)
        elif selectionType == 'working copy':
            for a in self.actions:
                if   a.text() == 'update'   : a.setVisible(True)
                elif a.text() == 'commit'   : a.setVisible(True)

    def initGui(self):

        self.info = QLabel()
        self.info.setText('No group selected')
        self.actions.append( self.iface.addToolBarWidget( self.info ) )

        # we can have a checkbox to either replace/add layers

        # this is not really nice since this is hidden in the interface
        # but nothing else is available to get a selected group in the legend
        self.legend = self.iface.mainWindow().findChild(QTreeWidget,'theMapLegend')
        self.legend.itemClicked.connect(self.onLegendClick)
        self.legend.itemChanged.connect(self.onLegendClick)

        self.actions.append( QAction(
            QIcon(os.path.dirname(__file__) + "/checkout.svg"),
            u"checkout", self.iface.mainWindow()) )
        self.actions[-1].setWhatsThis("checkout")
        self.actions[-1].triggered.connect(self.checkout)
        self.actions[-1].setVisible(False)

        self.actions.append( QAction(
            QIcon(os.path.dirname(__file__) + "/update.svg"),
            u"update", self.iface.mainWindow()) )
        self.actions[-1].setWhatsThis("update working copy")
        self.actions[-1].triggered.connect(self.update)
        self.actions[-1].setVisible(False)

        self.actions.append( QAction(
            QIcon(os.path.dirname(__file__) + "/commit.svg"),
            u"commit", self.iface.mainWindow()) )
        self.actions[-1].setWhatsThis("commit modifications")
        self.actions[-1].triggered.connect(self.commit)
        self.actions[-1].setVisible(False)

        self.actions.append( QAction(
            QIcon(os.path.dirname(__file__) + "/view.svg"),
            u"view", self.iface.mainWindow()) )
        self.actions[-1].setWhatsThis("see revision")
        self.actions[-1].triggered.connect(self.view)
        self.actions[-1].setVisible(False)

        self.actions.append( QAction(
            QIcon(os.path.dirname(__file__) + "/branch.svg"),
            u"branch", self.iface.mainWindow()) )
        self.actions[-1].setWhatsThis("create branch")
        self.actions[-1].triggered.connect(self.branch)
        self.actions[-1].setVisible(False)

        # add actions in menus
        for a in self.actions:
            self.iface.addToolBarIcon(a)

    def unload(self):
        # Remove the plugin menu item and icon
        for a in self.actions:
            self.iface.removeToolBarIcon(a)
        self.legend.itemClicked.disconnect(self.onLegendClick)
        self.legend.itemChanged.disconnect(self.onLegendClick)

    def branch(self):
        layer = QgsMapLayerRegistry.instance().mapLayer( self.currentLayers[0] )
        uri = QgsDataSourceURI(layer.source())
        m = re.match('(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
        schema = m.group(1) 
        base_branch = m.group(2) 
        base_rev = m.group(3) 
        assert(schema)
        d = QDialog()
        d.setWindowTitle('Enter branch name')
        layout = QVBoxLayout(d)
        buttonBox = QDialogButtonBox(d)
        buttonBox.setStandardButtons(QDialogButtonBox.Cancel|QDialogButtonBox.Ok)
        buttonBox.accepted.connect(d.accept)
        buttonBox.rejected.connect(d.reject)

        lineEdit = QLineEdit( d )
        layout.addWidget( lineEdit )
        layout.addWidget( buttonBox )
        if not d.exec_() : return
        branch = lineEdit.text() 

        if not branch:
            print 'aborted'
            return

        pcur = versioning_base.Db( psycopg2.connect(uri.connectionInfo()) ) 
        pcur.execute("SELECT * FROM "+schema+".revisions WHERE branch = '"+branch+"'") 
        if pcur.fetchone():
            pcur.close()
            QMessageBox.warning( self.iface.mainWindow(), "Warning", "Branch "+branch+' already exists.')
            return
        pcur.close()
        
        # get the commit message
        if not self.qCommitMsgDialog.exec_(): return
        commit_msg = self.commitMsgDialog.commitMessage.document().toPlainText()
        if not commit_msg:
            QMessageBox.warning(self.iface.mainWindow(), "Warning", "No commit message, aborting commit")
            print "aborted"
            return
        versioning_base.add_branch(uri.connectionInfo(), schema, branch, commit_msg, base_branch, base_rev )
        groupName = branch+' revision head'
        groupIdx = self.iface.legendInterface().addGroup( groupName )
        for layerId in reversed(self.currentLayers):
            layer = QgsMapLayerRegistry.instance().mapLayer(layerId)
            newUri = QgsDataSourceURI(layer.source())
            newUri.setDataSource(schema+'_'+branch+'_rev_head', 
                    newUri.table(), 
                    newUri.geometryColumn(),
                    newUri.sql(),
                    newUri.keyColumn())
            display_name =  QgsMapLayerRegistry.instance().mapLayer(layerId).name()
            newLayer = self.iface.addVectorLayer(newUri.uri(), display_name, 'postgres')
            self.iface.legendInterface().moveLayer( newLayer, groupIdx)
        pass

    def view(self):
        layer = QgsMapLayerRegistry.instance().mapLayer( self.currentLayers[0] )
        uri = QgsDataSourceURI(layer.source())
        m = re.match('(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
        schema = m.group(1) 
        assert(schema)
        d = QDialog()
        layout = QVBoxLayout(d)
        buttonBox = QDialogButtonBox(d)
        buttonBox.setStandardButtons(QDialogButtonBox.Cancel|QDialogButtonBox.Ok)
        buttonBox.accepted.connect(d.accept)
        buttonBox.rejected.connect(d.reject)

        pcur = versioning_base.Db( psycopg2.connect(uri.connectionInfo()) ) 
        pcur.execute("SELECT rev, commit_msg, branch, date, author FROM "+schema+".revisions") 
        revs = pcur.fetchall()
        pcur.close()
        tw = QTableWidget( d )
        tw.setRowCount(len(revs));
        tw.setColumnCount(5);
        tw.setSortingEnabled(True)
        tw.setHorizontalHeaderLabels(['Revision','Commit Message', 'Branch', 'Date','Author'])
        tw.verticalHeader().setVisible(False)
        for i,r in enumerate(revs):
            for j,item in enumerate(r):
                tw.setItem(i,j,QTableWidgetItem( str(item) ))
        layout.addWidget( tw )
        layout.addWidget( buttonBox )
        d.resize( 600, 300 )
        if not d.exec_() : return
        
        rows = set()
        for i in tw.selectedIndexes(): rows.add(i.row())
        for r in rows:
            branch = revs[r][2]
            rev = revs[r][0]
            versioning_base.add_revision_view(uri.connectionInfo(), schema, branch, rev )
            groupName = branch+' revision '+str(rev)
            groupIdx = self.iface.legendInterface().addGroup( groupName )
            for layerId in reversed(self.currentLayers):
                layer = QgsMapLayerRegistry.instance().mapLayer(layerId)
                newUri = QgsDataSourceURI(layer.source())
                newUri.setDataSource(schema+'_'+branch+'_rev_'+str(rev), 
                        newUri.table(), 
                        newUri.geometryColumn(),
                        newUri.sql(),
                        newUri.keyColumn())
                display_name =  QgsMapLayerRegistry.instance().mapLayer(layerId).name()
                newLayer = self.iface.addVectorLayer(newUri.uri(), display_name, 'postgres')
                self.iface.legendInterface().moveLayer( newLayer, groupIdx)

    def unresolvedConflicts(self):
        layer = QgsMapLayerRegistry.instance().mapLayer( self.currentLayers[0] )
        uri = QgsDataSourceURI(layer.source())

        unresolved = versioning_base.unresolvedConflicts( uri.database() )
        for c in unresolved:
            table = c+"_conflicts"
            if not QgsMapLayerRegistry.instance().mapLayersByName(table):
                self.iface.addVectorLayer("dbname="+uri.database()+" key=\"OGC_FID\" table=\""+table+"\"(GEOMETRY)",table,'spatialite')

        if unresolved: 
            QMessageBox.warning( self.iface.mainWindow(), "Warning", "Unresolved conflics for layer(s) "+', '.join(unresolved)+".\n\nPlease resolve conflicts by openning the conflict layer atribute table and deleting either 'mine' or 'theirs' before continuing.\n\nPlease note that the attribute table is not refreshed on save (known bug), once you have deleted the unwanted change in the conflict layer, close and reopen the attribute table to check it's empty.")
            return True
        else:
            return False

    def update(self):
        """merge modifiactions since last update into working copy"""
        print "update"
        if self.unresolvedConflicts(): return
        up_to_date = ""
        layer = QgsMapLayerRegistry.instance().mapLayer( self.currentLayers[0] )
        uri = QgsDataSourceURI(layer.source())

        versioning_base.update( uri.database() )
        rev = versioning_base.revision( uri.database() )

        if not self.unresolvedConflicts(): QMessageBox.information( self.iface.mainWindow(), "Notice", "Your are up to date with revision "+str(rev-1)+".")



    def checkout(self):
        """create working copy from versionned database layers"""
        # for each connection, we need the list of tables
        tables_for_conninfo = {}
        for layerId in self.currentLayers:
            layer = QgsMapLayerRegistry.instance().mapLayer( layerId )
            uri = QgsDataSourceURI(layer.source())
            conn_info = uri.connectionInfo()
            table =  uri.schema()+"."+uri.table()
            if conn_info in tables_for_conninfo: 
                tables_for_conninfo[conn_info].add(table)
            else: 
                tables_for_conninfo[conn_info] = set([table])

        filename = QFileDialog.getSaveFileName(self.iface.mainWindow(), 'Save Versionned Layers As', '.', '*.sqlite')
        if not filename:
            print "aborted"
            return

        if os.path.isfile(filename): os.remove(filename)

        for conn_info, tables in tables_for_conninfo.iteritems():
            print "checkin out ", tables, " from ", conn_info
            versioning_base.checkout( conn_info, list(tables), filename )
        
        # add layers from offline version
        groupName = 'working copy'
        if groupName in self.iface.legendInterface().groups():
            groupName = filename 
        groupIdx = self.iface.legendInterface().addGroup( groupName )
        for layerId in reversed(self.currentLayers):
            layer = QgsMapLayerRegistry.instance().mapLayer( layerId )
            uri = QgsDataSourceURI(layer.source())
            table = uri.table()
            display_name = layer.name()
            print "replacing ", display_name
            newLayer = self.iface.addVectorLayer("dbname="+filename+" key=\"OGC_FID\" table=\""+table+"_view\" (GEOMETRY)",display_name,'spatialite')
            self.iface.legendInterface().moveLayer( newLayer, groupIdx)


    def commit(self):
        """merge modifiactions into database"""
        print "commit"
        if self.unresolvedConflicts(): return

        layer = QgsMapLayerRegistry.instance().mapLayer( self.currentLayers[0] )
        uri = QgsDataSourceURI(layer.source())
        f = uri.database()
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

        nb_of_updated_layer = versioning_base.commit( f, commit_msg )
        if nb_of_updated_layer:

            QMessageBox.information(self.iface.mainWindow(), "Info", "You have successfully commited revision "+str( versioning_base.revision(f) ) )
        else:
            QMessageBox.information(self.iface.mainWindow(), "Info", "There was no modification to commit")

