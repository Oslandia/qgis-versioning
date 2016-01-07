
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

#from PyQt4.QtCore import QAction
from PyQt4.QtGui import QAction, QDialog, QDialogButtonBox, \
    QFileDialog, QIcon, QLabel, QLineEdit, QMessageBox, QTableWidget, \
    QTreeView, QTreeWidget, QVBoxLayout, QTableWidgetItem, QColor, QProgressBar,\
    QComboBox
from qgis.core import QgsCredentials, QgsDataSourceURI, QgsMapLayerRegistry, \
    QgsFeatureRequest
from PyQt4.QtCore import *
from qgis.gui import QgsMessageBar
import re
import os
import os.path
import psycopg2
from PyQt4 import uic
import versioning_base
import platform, sys

# Deactivate stdout (like output of print statements) because windows
# causes occasional "IOError [Errno 9] File descriptor error"
# Not needed when there is a way to run QGIS in console mode in Windows.
iswin = any(platform.win32_ver())
if iswin:
    sys.stdout = open(os.devnull, 'w')

# We start from layers coming from one or more postgis non-versioned schemata
# A widget group is displayed for each distinct schema
# (identified with 'dbname schema')
# The widget group contains a branch and version combobox extracted from layers
# You can only checkout head revision
# If you select a new branch, you have to enter the name and it will be
# created from either the current working copy or the current branch/rev
# If you select a revision, the corresponding view in the db will be
# created and the layers replaced

# The list of postgres connections can be found either in layers,
# or in working copy
# The list of working copies can be found either in layers or in filesystem

# BRANCHES have no underscore, no spaces

class Versioning:
    """Versioning postgis DB in QGIS"""

    def __init__(self, iface):
        # Save reference to the QGIS interface
        self.iface = iface
        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)

        self.q_commit_msg_dlg = QDialog(self.iface.mainWindow())
        self.q_commit_msg_dlg = uic.loadUi(self.plugin_dir+"/commit_msg.ui")
        self.commit_msg_dlg = ""

        self.current_layers = []
        self.actions = []
        self._pg_conn_info = ''
        self.current_group_idx = -1
        self.info = QLabel()

        # this is not really nice since this is hidden in the interface
        # but nothing else is available to get a selected group in the legend
        self.legend = self.iface.mainWindow().findChild( QTreeWidget,
                                                         'theMapLegend' )
        if self.legend: # qgis 2.2
            self.legend.itemClicked.connect(self.on_legend_click)
            self.legend.itemChanged.connect(self.on_legend_click)
        else: # qgis 2.4
            self.legend = self.iface.mainWindow().findChild( QTreeView, 'theLayerTreeView')
            self.legend.clicked.connect(self.on_legend_click)

    def pg_conn_info(self):
        """returns current postgis versioned DB connection info
        request credentials if needed"""
        if not self._pg_conn_info:
            # In the simple case: all pg layers share the same conn info
            # we set the conn info, if not, we ask for a connection
            # We then request credentials if necessary

            conn_info = ''
            for layer in self.iface.legendInterface().layers():
                if layer.providerType() == "postgres":
                    cni = QgsDataSourceURI(layer.source()).connectionInfo()
                    if not conn_info:
                        conn_info = cni
                    elif conn_info != cni:
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
                (success, user, passwd ) = QgsCredentials.instance().get(
                        conn_info, None, None )
                if success:
                    QgsCredentials.instance().put( conn_info, user, passwd )
                uri.setPassword(passwd)
                uri.setUsername(user)
            self._pg_conn_info = uri.connectionInfo()

        return self._pg_conn_info

    def get_pg_users_list(self):
        #get list of pg users to populate combobox used in commit msg dialog
        pg_users_list = versioning_base.get_pg_users_list(self.pg_conn_info())
        #print "pg_users_list = " + str(pg_users_list)
        self.q_commit_msg_dlg.pg_users_combobox.addItems (pg_users_list)

    def on_legend_click(self, current, column=0):
        "changes menu when user clicks on legend"
        self.current_group_idx = -1
        name = ''
        self.current_layers = []
        self.info.setText('Versioning : no group selected')
        for act in self.actions:
            if act.text() in ['checkout',
                              'update',
                              'commit',
                              'view',
                              'branch',
                              'historize' ]:
                act.setVisible(False)
        if current:
            try: # qgis 2.2
                name = current.text(0)
            except: #qgis 2.4
                name = current.data()
        # we could look if we have something in selected layers
        # but we prefer impose grouping, otherwise it'll be easy to make errors

        # need to get all layers including subgroups
        rel_map = {}
        for grp, lay in self.iface.legendInterface().groupLayerRelationship():
            rel_map[grp] = lay

        if not name or name not in rel_map: # not a group
            return

        group_idx = [i for i, x in
                enumerate(self.iface.legendInterface().groups()) if x == name]
        if len(group_idx) != 1:
            self.info.setText("Versioning : more than one group with this name")
            self.current_layers = []
            return
        [self.current_group_idx] = group_idx

        replaced = True
        while replaced:
            replaced = False
            for i, item in enumerate(rel_map[name]):
                if item in rel_map:
                    rel_map[name][i:i+1] = rel_map[item]
                    replaced = True

        self.current_layers = rel_map[name]
        # we should check that the selection is homogeneous
        previous_conn = ()
        for layer_id in self.current_layers:
            layer = QgsMapLayerRegistry.instance().mapLayer( layer_id )
            uri = QgsDataSourceURI(layer.source())
            if previous_conn:
                if (uri.database(), uri.schema()) != previous_conn:
                    self.current_layers = []
                    self.info.setText("Versioning : layers don't share db and schema")
                    return
            else:
                previous_conn = (uri.database(), uri.schema())

        if not self.current_layers:
            return

        if not len(previous_conn[0]):
            self.current_layers = []
            self.info.setText("Versioning : not versionable")
            return

        layer = QgsMapLayerRegistry.instance().mapLayer(
                self.current_layers[0] )
        uri = QgsDataSourceURI( layer.source() )
        selection_type = ''
        if layer.providerType() == "spatialite":
            rev = 0
            try:
                rev = versioning_base.revision( uri.database() )
            except:
                self.current_layers = []
                self.info.setText("Versioning : the selected group is not a working copy")
                return
            # We can split on "/" irrespective of OS because QgsDataSourceURI
            # normalises the path separator to "/"
            self.info.setText( uri.database().split("/")[-1] +' <b>working rev</b>='+str(rev))
            selection_type = 'working copy'
        if layer.providerType() == "postgres":
            mtch = re.match(r'(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
            if mtch:
                self.info.setText(uri.database()+' '+mtch.group(1)
                        +' branch='+mtch.group(2)+' rev='+mtch.group(3))
                if mtch.group(3) == 'head':
                    selection_type = 'head'
                else:
                    selection_type = 'versioned'
            else:
                # check if it's a working copy
                rev = 0
                try:
                    rev = versioning_base.pg_revision( self.pg_conn_info(),
                                                       uri.schema() )
                    selection_type = 'working copy'
                    self.info.setText( uri.database()+' '+uri.schema()
                            +' <b>working rev</b>='+str(rev) )
                except:
                    self.info.setText('Versioning : unversioned schema')
                    selection_type = 'unversioned'


        # refresh the available commands
        assert( selection_type )
        if selection_type == 'unversioned':
            for act in self.actions:
                if act.text() == 'historize':
                    act.setVisible(True)
        elif selection_type == 'versioned':
            for act in self.actions:
                if act.text() in ['view', 'branch']:
                    act.setVisible(True)
        elif selection_type == 'head':
            for act in self.actions:
                if act.text() in ['checkout', 'view', 'branch']:
                    act.setVisible(True)
        elif selection_type == 'working copy':
            for act in self.actions:
                if act.text() in ['update', 'commit']:
                    act.setVisible(True)

    def initGui(self):
        """Called once QGIS gui is loaded, before project is loaded"""

        self.info.setText('Versioning : no group selected')
        self.actions.append( self.iface.addToolBarWidget( self.info ) )

        # we could have a checkbox to either replace/add layers

        self.actions.append( QAction(
            QIcon(os.path.dirname(__file__) + "/historize.svg"),
            u"historize", self.iface.mainWindow()) )
        self.actions[-1].setWhatsThis("historize")
        self.actions[-1].triggered.connect(self.historize)
        self.actions[-1].setVisible(False)

        self.actions.append( QAction(
            QIcon(os.path.dirname(__file__) + "/checkout.svg"),
            u"checkout", self.iface.mainWindow()) )
        self.actions[-1].setToolTip ("spatialite checkout")
        self.actions[-1].setWhatsThis("checkout")
        self.actions[-1].triggered.connect(self.checkout)
        self.actions[-1].setVisible(False)

        self.actions.append( QAction(
            QIcon(os.path.dirname(__file__) + "/checkout_pg.svg"),
            u"checkout", self.iface.mainWindow()) )
        self.actions[-1].setToolTip ("postGIS checkout")
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
        for act in self.actions:
            self.iface.addToolBarIcon(act)

    def unload(self):
        """called when plugin is unloaded"""
        # Remove the plugin menu item and icon
        for act in self.actions:
            self.iface.removeToolBarIcon(act)
        try: # qgis 2.2
            self.legend.itemClicked.disconnect(self.on_legend_click)
            self.legend.itemChanged.disconnect(self.on_legend_click)
        except: # qgis 2.4
            self.legend.clicked.disconnect(self.on_legend_click)

    def branch(self):
        """create branch and import layers"""
        layer = QgsMapLayerRegistry.instance().mapLayer(
                self.current_layers[0] )
        uri = QgsDataSourceURI(layer.source())
        mtch = re.match(r'(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
        schema = mtch.group(1)
        base_branch = mtch.group(2)
        base_rev = mtch.group(3)
        assert(schema)
        dlg = QDialog()
        dlg.setWindowTitle('Enter branch name')
        layout = QVBoxLayout(dlg)
        button_box = QDialogButtonBox(dlg)
        button_box.setStandardButtons(
            QDialogButtonBox.Cancel|QDialogButtonBox.Ok)
        button_box.accepted.connect(dlg.accept)
        button_box.rejected.connect(dlg.reject)

        line_edit = QLineEdit( dlg )
        layout.addWidget( line_edit )
        layout.addWidget( button_box )
        if not dlg.exec_() :
            return
        branch = line_edit.text()

        if not branch:
            print 'aborted'
            return

        pcur = versioning_base.Db( psycopg2.connect(self.pg_conn_info()) )
        pcur.execute("SELECT * FROM "+schema+".revisions "
            "WHERE branch = '"+branch+"'")
        if pcur.fetchone():
            pcur.close()
            QMessageBox.warning( self.iface.mainWindow(), "Warning",
                    "Branch "+branch+' already exists.')
            return
        pcur.close()

        # get the commit message
        # get rid of the combobox asking for the pg username of committer
        self.q_commit_msg_dlg.pg_users_combobox.setVisible(False)
        self.q_commit_msg_dlg.pg_username_label.setVisible(False)
        if not self.q_commit_msg_dlg.exec_():
            return
        commit_msg = self.q_commit_msg_dlg.commitMessage.document().toPlainText()
        if not commit_msg:
            QMessageBox.warning(self.iface.mainWindow(), "Warning",
                "No commit message, aborting commit")
            print "aborted"
            return
        versioning_base.add_branch( uri.connectionInfo(),
                schema, branch, commit_msg, base_branch, base_rev )
        grp_name = branch+' revision head'
        grp_idx = self.iface.legendInterface().addGroup( grp_name )
        for layer_id in reversed(self.current_layers):
            layer = QgsMapLayerRegistry.instance().mapLayer(layer_id)
            new_uri = QgsDataSourceURI(layer.source())
            new_uri.setDataSource(schema+'_'+branch+'_rev_head',
                    new_uri.table(),
                    new_uri.geometryColumn(),
                    new_uri.sql(),
                    new_uri.keyColumn())
            display_name = QgsMapLayerRegistry.instance().mapLayer(layer_id).name()

            new_layer = self.iface.addVectorLayer(new_uri.uri().replace('()',''),
                    display_name, 'postgres')
            self.iface.legendInterface().moveLayer( new_layer, grp_idx)

    def view(self):
        """create view and import layers"""
        layer = QgsMapLayerRegistry.instance().mapLayer(
                self.current_layers[0] )
        uri = QgsDataSourceURI(layer.source())
        mtch = re.match(r'(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
        schema = mtch.group(1)
        assert(schema)
        dlg = QDialog()
        layout = QVBoxLayout(dlg)
        button_box = QDialogButtonBox(dlg)
        button_box.setStandardButtons(
                QDialogButtonBox.Cancel|QDialogButtonBox.Ok)
        button_box.accepted.connect(dlg.accept)
        button_box.rejected.connect(dlg.reject)

        user_msg1 = QgsMessageBar(dlg)
        user_msg1.pushInfo("Select:", "one [many] for single [multiple] "
        "revisions.  Fetching may take time.")

        pcur = versioning_base.Db( psycopg2.connect(self.pg_conn_info()) )
        pcur.execute("SELECT rev, commit_msg, branch, date, author "
            "FROM "+schema+".revisions")
        revs = pcur.fetchall()
        pcur.close()
        tblw = QTableWidget( dlg )
        tblw.setRowCount(len(revs))
        tblw.setColumnCount(6)
        tblw.setSortingEnabled(True)
        tblw.setHorizontalHeaderLabels(['Select','Revision', 'Commit Message',
                                      'Branch', 'Date', 'Author'])
        tblw.verticalHeader().setVisible(False)
        for i, rev in enumerate(revs):
            for j, item in enumerate(rev):
                chkBoxItem = QTableWidgetItem()
                chkBoxItem.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
                chkBoxItem.setCheckState(Qt.Unchecked)
                tblw.setItem(i, 0, chkBoxItem)
                tblw.setItem(i, j+1, QTableWidgetItem( str(item) ))

        layout.addWidget( user_msg1 )
        layout.addWidget( tblw )
        layout.addWidget( button_box )
        dlg.resize( 650, 300 )
        if not dlg.exec_() :
            return

        rows = set()
        revision_number_list = []
        for i in range(len(revs)):
            if  tblw.item(i,0).checkState():
                print "Revision "+ str(i + 1) +" will be fetched"
                revision_number_list.append(i + 1)
                rows.add(tblw.item(i,0).row())

        progressMessageBar = self.iface.messageBar().createMessage("Querying "
        "the database for revision(s) "+str(revision_number_list))
        progress = QProgressBar()
        progress.setMaximum(len(rows))
        progress.setAlignment(Qt.AlignLeft|Qt.AlignVCenter)
        progressMessageBar.layout().addWidget(progress)
        self.iface.messageBar().pushWidget(progressMessageBar, self.iface.messageBar().INFO)
        progress.setValue(0)

        for i, row in enumerate(rows):
            progress.setValue(i+1)
            branch = revs[row][2]
            rev = revs[row][0]
            versioning_base.add_revision_view(uri.connectionInfo(),
                    schema, branch, rev )
            grp_name = branch+' revision '+str(rev)
            grp_idx = self.iface.legendInterface().addGroup( grp_name )
            for layer_id in reversed(self.current_layers):
                layer = QgsMapLayerRegistry.instance().mapLayer(layer_id)
                new_uri = QgsDataSourceURI(layer.source())
                new_uri.setDataSource(schema+'_'+branch+'_rev_'+str(rev),
                        new_uri.table(),
                        new_uri.geometryColumn(),
                        new_uri.sql(),
                        new_uri.keyColumn())
                display_name =  QgsMapLayerRegistry.instance().mapLayer(layer_id).name()
                src = new_uri.uri().replace('()','')
                new_layer = self.iface.addVectorLayer( src,
                        display_name, 'postgres')
                self.iface.legendInterface().moveLayer( new_layer, grp_idx)
        self.iface.messageBar().clearWidgets()

    def unresolved_conflicts(self):
        """check for unresolved conflicts, add conflict layers if any"""
        layer = QgsMapLayerRegistry.instance().mapLayer(
                self.current_layers[0] )
        uri = QgsDataSourceURI(layer.source())

        if layer.providerType() == "spatialite":
            unresolved = versioning_base.unresolved_conflicts( uri.database() )
            for cflt in unresolved:
                table = cflt+"_conflicts"
                if not QgsMapLayerRegistry.instance().mapLayersByName(table):
                    #TODO detect if there is a geometry column
                    geom = '(GEOMETRY)' #if uri.geometryColumn() else ''
                    self.iface.addVectorLayer(
                            "dbname="+uri.database()+
                            " key=\"OGC_FID\" table=\""+table+"\" "+
                            geom,table,'spatialite')
        else: #postgres
            unresolved = versioning_base.pg_unresolved_conflicts(
                    uri.connectionInfo(), uri.schema() )
            for cflt in unresolved:
                table = cflt+"_conflicts"
                if not QgsMapLayerRegistry.instance().mapLayersByName(table):
                    new_uri = QgsDataSourceURI( uri.connectionInfo() )
                    print new_uri.uri()
                    new_uri.setDataSource(uri.schema(),
                            table,
                            uri.geometryColumn(),
                            uri.sql(),
                            uri.keyColumn())
                    src = new_uri.uri().replace('()','')
                    self.iface.addVectorLayer(src, table, 'postgres')

        if unresolved:
            QMessageBox.warning( self.iface.mainWindow(), "Warning",
                    "Unresolved conflics for layer(s) "+', '.join(unresolved)+
                    ".\n\nPlease resolve conflicts by opening the conflict "
                    "layer atribute table and deleting either 'mine' or "
                    "'theirs' before continuing.\n\n"
                    "Please note that the attribute table is not "
                    "refreshed on save (known bug), once you have deleted "
                    "the unwanted change in the conflict layer, close and "
                    "reopen the attribute table to check it's empty.")
            return True
        else:
            return False

    def update(self):
        """merge modifiactions since last update into working copy"""
        print "update"
        if self.unresolved_conflicts():
            return
        layer = QgsMapLayerRegistry.instance().mapLayer(
                self.current_layers[0] )
        uri = QgsDataSourceURI(layer.source())

        late_by = 0

        if layer.providerType() == "spatialite":
            late_by = versioning_base.late(
                    uri.database(), self.pg_conn_info() )
        else: # postgres
            late_by = versioning_base.pg_late(
                    self.pg_conn_info(), uri.schema() )
        if late_by:
            if layer.providerType() == "spatialite":
                versioning_base.update( uri.database(), self.pg_conn_info() )
                rev = versioning_base.revision( uri.database() )
            else: # postgres
                versioning_base.pg_update( uri.connectionInfo(), uri.schema() )
                rev = versioning_base.pg_revision(
                        uri.connectionInfo(), uri.schema() )

            # Force refresh of map
            if self.iface.mapCanvas().isCachingEnabled():
                self.iface.mapCanvas().clearCache()
                self.iface.mapCanvas().refresh()
            else:
                self.iface.mapCanvas().refresh()

            # Force refresh of rev number in menu text
            if layer.providerType() == "spatialite":
                self.info.setText( uri.database().split("/")[-1] +' <b>working rev</b>='+str(rev))
            else:
                self.info.setText( uri.database() +' <b>working rev</b>='+str(rev))

            if not self.unresolved_conflicts():
                QMessageBox.warning( self.iface.mainWindow(), "Warning",
                "Working copy was late by "+str(late_by)+" revision(s).\n"
                "Now up to date with remote revision "+str(rev-1)+".")
        else:
            if layer.providerType() == "spatialite":
                rev = versioning_base.revision( uri.database() )
            else: # postgres
                rev = versioning_base.pg_revision(
                        uri.connectionInfo(), uri.schema() )
            QMessageBox.information( self.iface.mainWindow(), "Info","Working "
            "copy already up to date with remote revision "+str(rev-1)+".")

    def historize(self):
        """version database"""
        uri = None
        conn_info = ''
        schema = ''
        for layer_id in self.current_layers:
            layer = QgsMapLayerRegistry.instance().mapLayer( layer_id )
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
                "Warning", "This will add 4 columns to all tables in schema "
                +schema+" (i.e. even to tables not in this project)",
                QMessageBox.Ok, QMessageBox.Cancel):
            print "aborted"
            return

        versioning_base.historize( self.pg_conn_info(), schema )

        grp_name = 'trunk revision head'
        grp_idx = self.iface.legendInterface().addGroup( grp_name )
        for layer_id in reversed(self.current_layers):
            layer = QgsMapLayerRegistry.instance().mapLayer(layer_id)
            new_uri = QgsDataSourceURI(layer.source())
            new_uri.setDataSource(schema+'_trunk_rev_head',
                    new_uri.table(),
                    new_uri.geometryColumn(),
                    new_uri.sql(),
                    new_uri.keyColumn())
            display_name = QgsMapLayerRegistry.instance().mapLayer(layer_id).name()
            src = new_uri.uri().replace('()','')
            new_layer = self.iface.addVectorLayer(src, display_name, 'postgres')
            self.iface.legendInterface().moveLayer( new_layer, grp_idx)
        self.iface.legendInterface().removeGroup( self.current_group_idx )
        self.current_layers = []

    def checkout(self):
        """create working copy from versioned database layers"""
        # for each connection, we need the list of tables
        tables_for_conninfo = []
        # for each layer, we need the list of user selected features to be
        # checked out; if a given layer has no user selected features, then all
        # features will be checked out
        user_selected_features = []
        uri = None
        conn_info = ''
        for layer_id in self.current_layers:
            layer = QgsMapLayerRegistry.instance().mapLayer( layer_id )
            uri = QgsDataSourceURI(layer.source())

            # Get actual PK fror corresponding table
            actual_table_pk = versioning_base.get_actual_pk( uri,self.pg_conn_info() )
            #print "Actual table pk = " + actual_table_pk

            layer_selected_features_ids = layer.selectedFeaturesIds()

            # Check if PK from view [uri.keyColumn()] matches actual PK. If not,
            # throw error.  We need the right PK from the view in order to use
            # the efficient selectedFeaturesIds().  selectedFeatures() or other
            # ways that lead to a list of QGSFeature objects do not scale well.
            if layer_selected_features_ids:
                if uri.keyColumn()!= actual_table_pk:
                    QMessageBox.warning(None,"Warning","Layer  \""+layer.name()+
                    " \" does not have the right primary key.\n\nCheckout will "
                    "proceed without the selected features subset.")
                    user_selected_features.append([])
                else:
                    QMessageBox.warning(None,"Warning","You will be checking out "
                    "the subset of "+str(len(layer_selected_features_ids))+" features "
                    "you selected in layer \""+layer.name()+"\".\n\nIf you want "
                    "the whole data set for that layer, abort checkout in the pop "
                    "up asking for a filename, unselect features and start over.")
                    user_selected_features.append(layer_selected_features_ids)
            else:
                user_selected_features.append([])
            if not conn_info:
                conn_info = uri.connectionInfo()
            else:
                assert(conn_info == uri.connectionInfo())
            table =  uri.schema()+"."+uri.table()
            tables_for_conninfo.append(table)

        filename = QFileDialog.getSaveFileName(self.iface.mainWindow(),
                'Save Versioned Layers As', '.', '*.sqlite')
        if not filename:
            print "aborted"
            return

        if os.path.isfile(filename):
            os.remove(filename)

        print "checking out ", tables_for_conninfo, " from ",uri.connectionInfo()
        versioning_base.checkout( self.pg_conn_info(),
                tables_for_conninfo, filename, user_selected_features )

        # add layers from offline version
        grp_name = 'working copy'
        if grp_name in self.iface.legendInterface().groups():
            grp_name = filename
        grp_idx = self.iface.legendInterface().addGroup( grp_name )
        for layer_id in reversed(self.current_layers):
            layer = QgsMapLayerRegistry.instance().mapLayer( layer_id )
            uri = QgsDataSourceURI(layer.source())
            table = uri.table()
            display_name = layer.name()
            print "replacing ", display_name
            geom = '(GEOMETRY)' if uri.geometryColumn() else ''
            new_layer = self.iface.addVectorLayer("dbname="+filename+
                    " key=\"OGC_FID\" table=\""+table+"_view\" "
                    +geom,display_name, 'spatialite')
            self.iface.legendInterface().moveLayer( new_layer, grp_idx)
        self.iface.legendInterface().setGroupExpanded( grp_idx, True )

    def checkout_pg(self):
        """create postgres working copy (schema) from versioned
        database layers"""
        # for each connection, we need the list of tables
        tables_for_conninfo = []
        uri = None
        conn_info = ''
        for layer_id in self.current_layers:
            layer = QgsMapLayerRegistry.instance().mapLayer( layer_id )
            uri = QgsDataSourceURI(layer.source())
            if not conn_info:
                conn_info = uri.connectionInfo()
            else:
                assert(conn_info == uri.connectionInfo())
            table =  uri.schema()+"."+uri.table()
            tables_for_conninfo.append(table)


        dlg = QDialog()
        dlg.setWindowTitle('Enter working copy schema name')
        layout = QVBoxLayout(dlg)
        button_box = QDialogButtonBox(dlg)
        button_box.setStandardButtons(
                QDialogButtonBox.Cancel|QDialogButtonBox.Ok)
        button_box.accepted.connect(dlg.accept)
        button_box.rejected.connect(dlg.reject)

        line_edit = QLineEdit( dlg )
        layout.addWidget( line_edit )
        layout.addWidget( button_box )
        if not dlg.exec_() :
            return
        working_copy_schema = line_edit.text()
        if not working_copy_schema:
            print "aborted"
            return

        print "checking out ", tables_for_conninfo, " from ", uri.connectionInfo()
        versioning_base.pg_checkout( self.pg_conn_info(),
                tables_for_conninfo, working_copy_schema )

        # add layers from offline version
        grp_idx = self.iface.legendInterface().addGroup( working_copy_schema )
        for layer_id in reversed(self.current_layers):
            layer = QgsMapLayerRegistry.instance().mapLayer( layer_id )
            new_uri = QgsDataSourceURI(layer.source())
            new_uri.setDataSource(working_copy_schema,
                    new_uri.table()+"_view",
                    new_uri.geometryColumn(),
                    new_uri.sql(),
                    new_uri.keyColumn())
            display_name =  QgsMapLayerRegistry.instance().mapLayer(layer_id).name()
            print "replacing ", display_name
            src = new_uri.uri().replace('()','')
            new_layer = self.iface.addVectorLayer(src, display_name, 'postgres')
            self.iface.legendInterface().moveLayer( new_layer, grp_idx)

    def commit(self):
        """merge modifications into database"""
        print "commit"
        if self.unresolved_conflicts():
            return

        layer = QgsMapLayerRegistry.instance().mapLayer(
                self.current_layers[0] )
        uri = QgsDataSourceURI(layer.source())

        late_by = 0
        if layer.providerType() == "spatialite":
            late_by = versioning_base.late(
                    uri.database(), self.pg_conn_info() )
        else:#postgres
            late_by = versioning_base.pg_late(
                    self.pg_conn_info(), uri.schema() )

        if late_by:
            QMessageBox.warning(self.iface.mainWindow(), "Warning",
                    "This working copy is not up to date (late by "
                    +str(late_by)+" commit(s)).\n\n"
                    "Please update before committing your modifications")
            print "aborted"
            return

        # Make sure the combobox is visible; could be made invisible by a
        # previous call to branch
        self.q_commit_msg_dlg.pg_users_combobox.setVisible(True)
        self.q_commit_msg_dlg.pg_username_label.setVisible(True)
        # Populate combobox with list of pg usernames
        nb_items_in_list = self.q_commit_msg_dlg.pg_users_combobox.count()
        if not(nb_items_in_list) :
            self.get_pg_users_list()
        # Better if we could have a QgsDataSourceURI.username()
        pg_username = self.pg_conn_info().split(' ')[3].replace("'","").split('=')[1]
        current_user_index = self.q_commit_msg_dlg.pg_users_combobox.findText(pg_username)
        # sets the current pg_user in the combobox to come
        current_user_combobox_item = self.q_commit_msg_dlg.pg_users_combobox.setCurrentIndex(current_user_index)

        # time to get the commit message
        if not self.q_commit_msg_dlg.exec_():
            return
        commit_msg = self.q_commit_msg_dlg.commitMessage.document().toPlainText()
        commit_pg_user = self.q_commit_msg_dlg.pg_users_combobox.itemText(self.q_commit_msg_dlg.pg_users_combobox.currentIndex())
        #print "commit_pg_user = " + commit_pg_user
        if not commit_msg:
            QMessageBox.warning(self.iface.mainWindow(), "Warning",
                    "No commit message, aborting commit")
            print "aborted"
            return

        nb_of_updated_layer = 0
        rev = 0
        if layer.providerType() == "spatialite":
            nb_of_updated_layer = versioning_base.commit( uri.database(),
                    commit_msg, self.pg_conn_info(),commit_pg_user )
            rev = versioning_base.revision(uri.database())
        else: # postgres
            nb_of_updated_layer = versioning_base.pg_commit(
                    uri.connectionInfo(), uri.schema(), commit_msg )
            rev = versioning_base.pg_revision(
                    uri.connectionInfo(), uri.schema())

        if nb_of_updated_layer:
            #self.iface.messageBar().pushMessage("Info",
            #"You have successfully committed revision "+str( rev ), duration=10)
            QMessageBox.information(self.iface.mainWindow(), "Info",
            "You have successfully committed remote revision "+str( rev-1 ) )

            # Force refresh of rev number in menu text
            if layer.providerType() == "spatialite":
                self.info.setText( uri.database().split("/")[-1] +' <b>working rev</b>='+str(rev))
            else:
                self.info.setText( uri.database() +' <b>working rev</b>='+str(rev))
        else:
            #self.iface.messageBar().pushMessage("Info",
            #"There was no modification to commit", duration=10)
            QMessageBox.information(self.iface.mainWindow(), "Info",
            "There was no modification to commit")
