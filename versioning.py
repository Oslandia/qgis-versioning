
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
        self.pg_conn_info = ''
        self.current_group_idx = -1

    def pgConnInfo(self):
        if not self.pg_conn_info:
            # In the simple case: all pg layers share the same conn info
            # we set the conn info, if not, we ask for a connection
            # We then request credentials if necessary

            conn_info = ''
            for layer in self.iface.legendInterface().layers():
               if layer.providerType() == "postgres":
                   ci = QgsDataSourceURI(layer.source()).connectionInfo()
                   if not conn_info:
                       conn_info = ci
                   elif conn_info != ci:
                       conn_info = 'heterogeneous'
            if conn_info == 'heterogeneous':
                assert(False) # TODO request connection

            uri = QgsDataSourceURI( conn_info )
            conn = None
            try:
                conn = psycopg2.connect(conn_info)
            except:
                conn = None
            if not conn:
                #print "Case when the pass/user are not saved in the project"
                (success, user, passwd ) = QgsCredentials.instance().get( connInfo, None, None )
                if success:
                    QgsCredentials.instance().put( connInfo, user, passwd )
                uri.setPassword(passwd)
                uri.setUsername(user)
            self.pg_conn_info = uri.connectionInfo()

        return self.pg_conn_info

    def onLegendClick(self, current, column=0):
        self.current_group_idx = -1
        name = ''
        self.currentLayers = []
        self.info.setText('No group selected')
        for a in self.actions:
            if   a.text() == 'checkout' : a.setVisible(False)
            elif a.text() == 'update'   : a.setVisible(False)
            elif a.text() == 'commit'   : a.setVisible(False)
            elif a.text() == 'view'     : a.setVisible(False)
            elif a.text() == 'branch'   : a.setVisible(False)
            elif a.text() == 'historize': a.setVisible(False)
        if current: 
            name = current.text(0)
        # we could look if we have something in selected layers
        # but we prefer impose grouping, otherwize it'll be easy to make errors

        # need to get all layers including subgroups
        rel = self.iface.legendInterface().groupLayerRelationship()
        relMap = {}
        for g,l in rel: relMap[g] = l

        if not name or name not in relMap: # not a group
            return

        group_idx = [i for i,x in enumerate(self.iface.legendInterface().groups()) if x == name]
        if len(group_idx) != 1:
            self.info.setText("More than one group with this name")
            self.currentLayers = []
            return
        [self.current_group_idx] = group_idx

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
                    self.info.setText("Layers don't share db and schema")
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
                # check if it's a working copy
                rev = 0
                try: 
                    rev = versioning_base.pg_revision( self.pgConnInfo(), uri.schema() )
                    selectionType = 'working copy'
                    self.info.setText( uri.database()+' '+uri.schema() +' rev='+str(rev))
                except:
                    currentLayers = []
                    self.info.setText('Unversioned schema')
                    selectionType = 'unversioned'
        

        # refresh the available commands
        assert( selectionType )
        if selectionType == 'unversioned':
            for a in self.actions:
                if   a.text() == 'historize': a.setVisible(True)
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

        # we could have a checkbox to either replace/add layers

        # this is not really nice since this is hidden in the interface
        # but nothing else is available to get a selected group in the legend
        self.legend = self.iface.mainWindow().findChild(QTreeWidget,'theMapLegend')
        self.legend.itemClicked.connect(self.onLegendClick)
        self.legend.itemChanged.connect(self.onLegendClick)

        self.actions.append( QAction(
            QIcon(os.path.dirname(__file__) + "/historize.svg"),
            u"historize", self.iface.mainWindow()) )
        self.actions[-1].setWhatsThis("historize")
        self.actions[-1].triggered.connect(self.historize)
        self.actions[-1].setVisible(False)

        self.actions.append( QAction(
            QIcon(os.path.dirname(__file__) + "/checkout.svg"),
            u"checkout", self.iface.mainWindow()) )
        self.actions[-1].setWhatsThis("checkout")
        self.actions[-1].triggered.connect(self.checkout)
        self.actions[-1].setVisible(False)

        self.actions.append( QAction(
            QIcon(os.path.dirname(__file__) + "/checkout_pg.svg"),
            u"checkout", self.iface.mainWindow()) )
        self.actions[-1].setWhatsThis("checkout postgres")
        self.actions[-1].triggered.connect(self.checkout_pg)
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

        pcur = versioning_base.Db( psycopg2.connect(self.pgConnInfo()) ) 
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
            
            newLayer = self.iface.addVectorLayer(newUri.uri().replace('()',''), display_name, 'postgres')
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

        pcur = versioning_base.Db( psycopg2.connect(self.pgConnInfo()) ) 
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
                newLayer = self.iface.addVectorLayer(newUri.uri().replace('()',''), display_name, 'postgres')
                self.iface.legendInterface().moveLayer( newLayer, groupIdx)

    def unresolved_conflicts(self):
        layer = QgsMapLayerRegistry.instance().mapLayer( self.currentLayers[0] )
        uri = QgsDataSourceURI(layer.source())

        if layer.providerType() == "spatialite":
            unresolved = versioning_base.unresolved_conflicts( uri.database() )
            for c in unresolved:
                table = c+"_conflicts"
                if not QgsMapLayerRegistry.instance().mapLayersByName(table):
                    #TODO detect if there is a geometry column
                    geom = '(GEOMETRY)' #if uri.geometryColumn() else ''
                    self.iface.addVectorLayer("dbname="+uri.database()+" key=\"OGC_FID\" table=\""+table+"\" "+geom,table,'spatialite')
        else: #postgres
            unresolved = versioning_base.pg_unresolved_conflicts( uri.connectionInfo(), uri.schema() )
            for c in unresolved:
                table = c+"_conflicts"
                if not QgsMapLayerRegistry.instance().mapLayersByName(table):
                    newUri = QgsDataSourceURI( uri.connectionInfo() )
                    print newUri.uri()
                    newUri.setDataSource(uri.schema(), 
                            table, 
                            uri.geometryColumn(),
                            uri.sql(),
                            uri.keyColumn())
                    self.iface.addVectorLayer(newUri.uri().replace('()',''),table,'postgres')

        if unresolved: 
            QMessageBox.warning( self.iface.mainWindow(), "Warning", "Unresolved conflics for layer(s) "+', '.join(unresolved)+".\n\nPlease resolve conflicts by openning the conflict layer atribute table and deleting either 'mine' or 'theirs' before continuing.\n\nPlease note that the attribute table is not refreshed on save (known bug), once you have deleted the unwanted change in the conflict layer, close and reopen the attribute table to check it's empty.")
            return True
        else:
            return False

    def update(self):
        """merge modifiactions since last update into working copy"""
        print "update"
        if self.unresolved_conflicts(): return
        up_to_date = ""
        layer = QgsMapLayerRegistry.instance().mapLayer( self.currentLayers[0] )
        uri = QgsDataSourceURI(layer.source())

        if layer.providerType() == "spatialite":
            versioning_base.update( uri.database(), self.pgConnInfo() )
            rev = versioning_base.revision( uri.database() )
        else: # postgres
            versioning_base.pg_update( uri.connectionInfo(), uri.schema() )
            rev = versioning_base.pg_revision( uri.connectionInfo(), uri.schema() )

        if not self.unresolved_conflicts(): QMessageBox.information( self.iface.mainWindow(), "Notice", "Your are up to date with revision "+str(rev-1)+".")



    def historize(self):
        """version database"""
        uri = None
        conn_info = ''
        schema = ''
        for layerId in self.currentLayers:
            layer = QgsMapLayerRegistry.instance().mapLayer( layerId )
            uri = QgsDataSourceURI(layer.source())
            if not conn_info:
                conn_info = uri.connectionInfo()
            else:
                assert(conn_info == uri.connectionInfo())
            if not schema:
                schema =  uri.schema()
            else:
                assert( schema == uri.schema() )

        if QMessageBox.Ok != QMessageBox.warning(self.iface.mainWindow(), 
                "Warning", "This will add 4 columns to all tables in schema "+schema+" (i.e. even to tables not in this project)", QMessageBox.Ok, QMessageBox.Cancel): 
            print "aborted"
            return

        versioning_base.historize( self.pgConnInfo(), schema )

        groupName = 'trunk revision head'
        groupIdx = self.iface.legendInterface().addGroup( groupName )
        for layerId in reversed(self.currentLayers):
            layer = QgsMapLayerRegistry.instance().mapLayer(layerId)
            newUri = QgsDataSourceURI(layer.source())
            newUri.setDataSource(schema+'_trunk_rev_head', 
                    newUri.table(), 
                    newUri.geometryColumn(),
                    newUri.sql(),
                    newUri.keyColumn())
            display_name =  QgsMapLayerRegistry.instance().mapLayer(layerId).name()
            
            newLayer = self.iface.addVectorLayer(newUri.uri().replace('()',''), display_name, 'postgres')
            self.iface.legendInterface().moveLayer( newLayer, groupIdx)
        self.iface.legendInterface().removeGroup( self.current_group_idx )
        self.currentLayers = []

    def checkout(self):
        """create working copy from versionned database layers"""
        # for each connection, we need the list of tables
        tables_for_conninfo = []
        uri = None
        conn_info = ''
        for layerId in self.currentLayers:
            layer = QgsMapLayerRegistry.instance().mapLayer( layerId )
            uri = QgsDataSourceURI(layer.source())
            if not conn_info:
                conn_info = uri.connectionInfo()
            else:
                assert(conn_info == uri.connectionInfo())
            table =  uri.schema()+"."+uri.table()
            tables_for_conninfo.append(table)

        filename = QFileDialog.getSaveFileName(self.iface.mainWindow(), 'Save Versionned Layers As', '.', '*.sqlite')
        if not filename:
            print "aborted"
            return

        if os.path.isfile(filename): os.remove(filename)

        print "checkin out ", tables_for_conninfo, " from ", uri.connectionInfo()
        versioning_base.checkout( self.pgConnInfo(), tables_for_conninfo, filename )
        
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
            geom = '(GEOMETRY)' if uri.geometryColumn() else ''
            newLayer = self.iface.addVectorLayer("dbname="+filename+" key=\"OGC_FID\" table=\""+table+"_view\" "+geom,display_name,'spatialite')
            self.iface.legendInterface().moveLayer( newLayer, groupIdx)


    def checkout_pg(self):
        """create postgres working copy (schema) from versionned database layers"""
        # for each connection, we need the list of tables
        tables_for_conninfo = []
        uri = None
        conn_info = ''
        for layerId in self.currentLayers:
            layer = QgsMapLayerRegistry.instance().mapLayer( layerId )
            uri = QgsDataSourceURI(layer.source())
            if not conn_info:
                conn_info = uri.connectionInfo()
            else:
                assert(conn_info == uri.connectionInfo())
            table =  uri.schema()+"."+uri.table()
            tables_for_conninfo.append(table)


        d = QDialog()
        d.setWindowTitle('Enter working copy schema name')
        layout = QVBoxLayout(d)
        buttonBox = QDialogButtonBox(d)
        buttonBox.setStandardButtons(QDialogButtonBox.Cancel|QDialogButtonBox.Ok)
        buttonBox.accepted.connect(d.accept)
        buttonBox.rejected.connect(d.reject)

        lineEdit = QLineEdit( d )
        layout.addWidget( lineEdit )
        layout.addWidget( buttonBox )
        if not d.exec_() : return
        working_copy_schema = lineEdit.text() 
        if not working_copy_schema:
            print "aborted"
            return

        print "checkin out ", tables, " from ", uri.connectionInfo()
        versioning_base.pg_checkout( self.pgConnInfo(), tables_for_conninfo, working_copy_schema )
        
        # add layers from offline version
        groupIdx = self.iface.legendInterface().addGroup( working_copy_schema )
        for layerId in reversed(self.currentLayers):
            layer = QgsMapLayerRegistry.instance().mapLayer( layerId )
            newUri = QgsDataSourceURI(layer.source())
            newUri.setDataSource(working_copy_schema, 
                    newUri.table()+"_view", 
                    newUri.geometryColumn(),
                    newUri.sql(),
                    newUri.keyColumn())
            display_name =  QgsMapLayerRegistry.instance().mapLayer(layerId).name()
            print "replacing ", display_name
            newLayer = self.iface.addVectorLayer(newUri.uri().replace('()',''),display_name,'postgres')
            self.iface.legendInterface().moveLayer( newLayer, groupIdx)


    def commit(self):
        """merge modifiactions into database"""
        print "commit"
        if self.unresolved_conflicts(): return

        layer = QgsMapLayerRegistry.instance().mapLayer( self.currentLayers[0] )
        uri = QgsDataSourceURI(layer.source())

        late_by = 0
        if layer.providerType() == "spatialite":
            late_by = versioning_base.late( uri.database(), self.pgConnInfo() )
        else:#postgres
            late_by = versioning_base.pg_late( self.pgConnInfo(), uri.schema() )

        if late_by: 
            QMessageBox.warning(self.iface.mainWindow(), "Warning", "This working copy is not up to date (late by "+str(late_by)+" commit(s)).\n\nPlease update before commiting your modifications")
            print "aborted"
            return

        # time to get the commit message
        if not self.qCommitMsgDialog.exec_(): return
        commit_msg = self.commitMsgDialog.commitMessage.document().toPlainText()
        if not commit_msg:
            QMessageBox.warning(self.iface.mainWindow(), "Warning", "No commit message, aborting commit")
            print "aborted"
            return

        nb_of_updated_layer = 0
        rev = 0
        if layer.providerType() == "spatialite":
            nb_of_updated_layer = versioning_base.commit( uri.database(), commit_msg, self.pgConnInfo() )
            rev = versioning_base.revision(uri.database())
        else: # postgres
            nb_of_updated_layer = versioning_base.pg_commit(uri.connectionInfo(), uri.schema(), commit_msg )
            rev = versioning_base.pg_revision(uri.connectionInfo(), uri.schema())

        if nb_of_updated_layer:
            QMessageBox.information(self.iface.mainWindow(), "Info", "You have successfully commited revision "+str( rev ) )
        else:
            QMessageBox.information(self.iface.mainWindow(), "Info", "There was no modification to commit")

