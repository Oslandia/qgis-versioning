
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

from __future__ import absolute_import

import re
import os
import os.path
import psycopg2
import platform
import sys

from PyQt5 import uic
from PyQt5.QtWidgets import QAction, QDialog, QDialogButtonBox, \
    QFileDialog, QLabel, QLineEdit, QMessageBox, QTableWidget, \
    QTreeView, QTreeWidget, QVBoxLayout, QTableWidgetItem, QProgressBar,\
    QCheckBox, QComboBox
from PyQt5.QtGui import QIcon, QColor, QDesktopServices
from PyQt5.QtCore import QSettings, QObject, QUrl, Qt

from qgis.gui import QgsMessageBar
from qgis.utils import showPluginHelp
from qgis.core import QgsCredentials, QgsDataSourceUri, QgsProject, \
    QgsFeatureRequest, QgsWkbTypes, QgsFeature, QgsGeometry, QgsPoint, QgsSymbol, \
    QgsRuleBasedRenderer, QgsLayerTreeNode, QgsVectorLayer, Qgis

from .versioningDB import versioning


CONN = "PostgreSQL/connections"

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


class Plugin(QObject):
    """Versioning postgis DB in QGIS"""

    def __init__(self, iface):
        super(Plugin, self).__init__()
        # Save reference to the QGIS interface
        self.iface = iface
        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)

        self.q_commit_msg_dlg = QDialog(self.iface.mainWindow())
        self.q_commit_msg_dlg = uic.loadUi(self.plugin_dir+"/commit_msg.ui")
        self.commit_msg_dlg = ""

        self.q_view_dlg = QDialog(self.iface.mainWindow())
        self.q_view_dlg = uic.loadUi(self.plugin_dir+"/revision_dialog.ui")

        self.current_layers = []
        self.toolbar = None
        self.actions = []
        self._pg_conn_info = ''
        self.current_group = None
        self.info = None

        self.iface.layerTreeView().clicked.connect(self.on_legend_click)

        self.versioning = None

    def enable_diffmode(self):
        '''This function enables the diffmode checkbox iif the number of checked
        revision == 2.  The reason is that we want to apply diff styles to
        features only between two revisions.  When the checkbox is enabled, users
        can check it.  If it was checked at one point and the number of selected
        revisions != 2, then it is unchecked and disabled.
        Intended use in : versioning.view
        '''
        # print("in enable_diffmode")
        nb_checked_revs = 0
        for i in range(self.q_view_dlg.tblw.rowCount()):
            # check if tblw.item(0,0) is not None (bug with Qt slots ?)
            if self.q_view_dlg.tblw.item(0, 0):
                if self.q_view_dlg.tblw.item(i, 0).checkState():
                    nb_checked_revs += 1
            else:
                return

        if nb_checked_revs == 2:
            self.q_view_dlg.diffmode_chk.setEnabled(True)
        else:
            self.q_view_dlg.diffmode_chk.setCheckState(Qt.Unchecked)
            self.q_view_dlg.diffmode_chk.setEnabled(False)

    def check_branches(self):
        ''' In the comparison mode (diffmode), only two revisions are compared.
        Both revisions must be on the same branch for comparison to happen.  If
        that is not the case, branch names of the revision items are highlighted.
        '''
        # print("in check_branches")
        if self.q_view_dlg.diffmode_chk.isChecked():
            # print("Checkbox is checked")
            branches = []
            indexes = []
            for i in range(self.q_view_dlg.tblw.rowCount()):
                if self.q_view_dlg.tblw.item(i, 0).checkState():
                    branches.append(self.q_view_dlg.tblw.item(i, 3).text())
                    indexes.append(i)
            # print("Compared branches are " + branches[0] + ", " + branches[1])

            if branches[0] != branches[1]:
                print("Branches are not equal")
                self.q_view_dlg.diffmode_chk.setCheckState(Qt.Unchecked)
                self.iface.messageBar().pushMessage("Warning",
                                                    "Selected revisions cannot be compared because they are not on "
                                                    "the same branch.", level=Qgis.Warning, duration=5)
                # Highlight branch items in table
                # Ideally, find a way to temporarily highlight
                self.q_view_dlg.tblw.item(
                    indexes[0], 3).setBackground(QColor(255, 255, 0))
                self.q_view_dlg.tblw.item(
                    indexes[1], 3).setBackground(QColor(255, 255, 0))
        return

    def mem_layer_uri(self, pg_layer):
        '''Final string concatenation to get a proper memory layer uri.  Example:
        "Point?crs=epsg:4326&field=id:integer&field=name:string(20)&index=yes"
        Geometry identifiers supported in memory layers :
        Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon
        Intended use in : versioning.view
        '''
        mem_uri = 'Unknown'
        srid = str(QgsDataSourceUri(pg_layer.source()).srid())
        if pg_layer.wkbType() == QgsWkbTypes.Point:
            # print("Layer \"" + pg_layer.name()+ "\" is a point layer")
            mem_uri = "Point?crs=epsg:" + srid + "&" + \
                versioning.mem_field_names_types(pg_layer) + "&index=yes"
        if pg_layer.wkbType() == QgsWkbTypes.LineString:
            # print("Layer \"" + pg_layer.name()+ "\" is a linestring layer")
            mem_uri = "LineString?crs=epsg:" + srid + "&" + \
                versioning.mem_field_names_types(pg_layer) + "&index=yes"
        if pg_layer.wkbType() == QgsWkbTypes.Polygon:
            # print("Layer \"" + pg_layer.name()+ "\" is a polygon layer")
            mem_uri = "Polygon?crs=epsg:" + srid + "&" + \
                versioning.mem_field_names_types(pg_layer) + "&index=yes"
        if pg_layer.wkbType() == QgsWkbTypes.MultiPoint:
            # print("Layer \"" + pg_layer.name()+ "\" is a multi-point layer")
            mem_uri = "MultiPoint?crs=epsg:" + srid + "&" + \
                versioning.mem_field_names_types(pg_layer) + "&index=yes"
        if pg_layer.wkbType() == QgsWkbTypes.MultiLineString:
            # print("Layer \"" + pg_layer.name()+ "\" is a multi-linestring layer")
            mem_uri = "MultiLineString?crs=epsg:" + srid + "&" + \
                versioning.mem_field_names_types(pg_layer) + "&index=yes"
        if pg_layer.wkbType() == QgsWkbTypes.MultiPolygon:
            # print("Layer \"" + pg_layer.name()+ "\" is a multi-polygon layer")
            mem_uri = "MultiPolygon?crs=epsg:" + srid + "&" + \
                versioning.mem_field_names_types(pg_layer) + "&index=yes"
        return mem_uri

    def pg_conn_info(self):
        """returns current postgis versioned DB connection info
        request credentials if needed"""
        if not self._pg_conn_info:
            # In the simple case: all pg layers share the same conn info
            # we set the conn info, if not, we ask for a connection
            # We then request credentials if necessary

            conn_info = ''
            for layer in QgsProject.instance().mapLayers().values():
                if layer.providerType() == "postgres":
                    cni = QgsDataSourceUri(layer.source()).connectionInfo()
                    if not conn_info:
                        conn_info = cni
                    elif conn_info != cni:
                        conn_info = 'heterogeneous'
            if conn_info == 'heterogeneous':
                assert(False)  # TODO request connection

            uri = QgsDataSourceUri(conn_info)
            conn = None
            try:
                conn = psycopg2.connect(conn_info)
            except:
                conn = None
            if not conn:
                # print("Case when the pass/user are not saved in the project")
                (success, user, passwd) = QgsCredentials.instance().get(
                    conn_info, None, None)
                if success:
                    QgsCredentials.instance().put(conn_info, user, passwd)
                uri.setPassword(passwd)
                uri.setUsername(user)
            self._pg_conn_info = uri.connectionInfo()

        return self._pg_conn_info

    def get_pg_users_list(self):
        # get list of pg users to populate combobox used in commit msg dialog
        pg_users_list = versioning.get_pg_users_list(self.pg_conn_info())
        # print("pg_users_list = " + str(pg_users_list))
        self.q_commit_msg_dlg.pg_users_combobox.addItems(pg_users_list)

    def is_pgDistant(self, layer):
        if layer.providerType() != 'postgres':
            return False

        uri = QgsDataSourceUri(layer.source())
        con = psycopg2.connect(self.get_conn_from_uri(uri))
        cur = con.cursor()
        cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}' and table_name='wcs_con'""".format(
            schema=uri.schema()))
        res = cur.fetchone()

        if res:
            return True

        return False

    def get_conn_from_uri(self, uri):
        """Returns a connection info from an URI"""

        pg_conn_info = None
        if len(uri.service()) != 0:
            pg_conn_info = uri.service()
        else:
            pg_conn_info = "dbname='{}' user='{}' host='{}' port='{}' password='{}'".format(
                uri.database(), uri.username(), uri.host(), uri.port(), uri.password())

        return pg_conn_info

    def get_conn_from_settings(self, dbname):
        """Retruns a connection info and data from a dbname saved in QgsSettings"""
        qs = QSettings()
        conn_dict = {}
        qs.beginGroup(CONN + '/' + dbname)
        pg_conn_info = None
        if qs.value('service') :
            conn_dict['service'] = qs.value('service')
            pg_conn_info = "service={}".format(conn_dict['service'])
        else:
            conn_dict['database'] = qs.value('database', dbname)
            conn_dict['username'] = qs.value('username', "postgres")
            print("username={}".format(conn_dict['username']))
            conn_dict['host'] = qs.value('host', "127.0.0.1")
            conn_dict['port'] = qs.value('port', "5432")
            conn_dict['password'] = qs.value('password', '')
            pg_conn_info = "dbname='{}' user='{}' host='{}' port='{}' password='{}'".format(
                conn_dict['database'], conn_dict['username'], conn_dict['host'],
                conn_dict['port'], conn_dict['password'])
            
        return (pg_conn_info, conn_dict)

    def selectDatabase(self):
        dlg = QDialog()
        dlg.setWindowTitle('Choose the database')
        layout = QVBoxLayout(dlg)
        button_box = QDialogButtonBox(dlg)
        button_box.setStandardButtons(
            QDialogButtonBox.Cancel | QDialogButtonBox.Ok)
        button_box.accepted.connect(dlg.accept)
        button_box.rejected.connect(dlg.reject)

        connectionBox = QComboBox(dlg)
        qs = QSettings()
        qs.beginGroup(CONN)
        connections = qs.childGroups()
        connectionBox.addItems(connections)
        qs.endGroup()

        layout.addWidget(connectionBox)
        layout.addWidget(button_box)
        if not dlg.exec_():
            return None

        return connectionBox.currentText()


    def __get_layers_from_node(self, node):
        """
        Recursively get layers from given node and return them
        """

        if node.nodeType() == QgsLayerTreeNode.NodeLayer :
            return [node.layer()]
        
        else :
            nodes = []
            for child in node.children():
                nodes += self.__get_layers_from_node(child)

            return nodes

    def __compute_selection_type(self):
        """
        Compute and return a tuple (selection type, mergeable)
        """

        # we should check that the selection is homogeneous
        previous_conn = ()
        for layer in self.current_layers:
            uri = QgsDataSourceUri(layer.source())
            if previous_conn:
                if (uri.database(), uri.schema()) != previous_conn:
                    self.current_layers = []
                    self.info.setText(
                        "Versioning : layers don't share db and schema")
                    return (None, None)
            else:
                previous_conn = (uri.database(), uri.schema())

        if not self.current_layers:
            return (None, None)

        if not len(previous_conn[0]):
            self.current_layers = []
            self.info.setText("Versioning : not versionable")
            return (None, None)

        layer = self.current_layers[0]
        uri = QgsDataSourceUri(layer.source())
        if layer.providerType() == "spatialite":
            rev = 0
            try:
                if not self.versioning:
                    self.versioning = versioning.spatialite(
                        uri.database(), self.pg_conn_info())
                rev = self.versioning.revision()
            except:
                self.current_layers = []
                self.info.setText(
                    "Versioning : the selected group is not a working copy")
                return (None, None)
            # We can split on "/" irrespective of OS because QgsDataSourceUri
            # normalises the path separator to "/"
            self.info.setText(uri.database().split(
                "/")[-1] + ' <b>working rev</b>='+str(rev))
            return ('working copy', False)

        if layer.providerType() == "postgres":
            # IF schema contain table wcs_con is a pg distant versioning
            ret = self.is_pgDistant(layer)
            if ret:
                try:
                    if not self.versioning: 
                        out = self.selectDatabase()
                        (pg_conn_info_out, conn_dict) = self.get_conn_from_settings(out)
                        self.versioning = versioning.pgLocal(
                            pg_conn_info_out, uri.schema(), self.get_conn_from_uri(uri))
                        self._pg_conn_info = pg_conn_info_out
                    rev = self.versioning.revision()
                    self.info.setText(uri.database()+' '+uri.schema()
                                      + ' <b>working rev</b>='+str(rev))
                    return ('working copy', False)
                except:
                    self.info.setText(
                        'Versioning : the selected group is not a working copy')
                    return ('unversioned', False)
            else:
                mtch = re.match(r'(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
                if mtch:
                    self.info.setText(uri.database()+' '+mtch.group(1)
                                      + ' branch='+mtch.group(2)+' rev='+mtch.group(3))
                    mergeable = mtch.group(2) != 'trunk'
                    if mtch.group(3) == 'head':
                        return ('head', mergeable)
                    else:
                        return ('versioned', mergeable)
                else:
                    # check if it's a working copy
                    rev = 0
                    try:
                        if not self.versioning:
                            self.versioning = versioning.pgServer(
                                self.pg_conn_info(), uri.schema())
                        
                        rev = self.versioning.revision()
                        self.info.setText(uri.database()+' '+uri.schema()
                                          + ' <b>working rev</b>='+str(rev))
                        return ('working copy', False)
                    except:
                        self.info.setText('Versioning : unversioned schema')
                        return ('unversioned', False)
        
    def on_legend_click(self, layer):
        "changes menu when user clicks on legend"

        # Node has to be a group to be versionned
        node = self.iface.layerTreeView().currentNode()
        if node.nodeType() != QgsLayerTreeNode.NodeGroup:
            self.info.setText("Versioning : No group selected")
            selection_type, mergeable = (None, False)
        else:
            self.current_group = node
            self.current_layers = self.__get_layers_from_node(node)
            selection_type, mergeable = self.__compute_selection_type()

        # Reset actions visibility
        for act in self.actions:
            act.setVisible(False)
            
        selection_type2actions = {
            'unversioned' : ['historize'],
            'versioned' : ['view', 'branch'],
            'head' : ['checkout', 'view', 'branch', 'archive'],
            'working copy' : ['update', 'commit']
        }

        if selection_type not in selection_type2actions:
            return

        possible_actions = selection_type2actions[selection_type]
        if mergeable:
            possible_actions += 'merge'

        # Update actions visibility
        for act in self.actions:
            if act.text() in possible_actions:
                act.setVisible(True)
        
    def initGui(self):
        """Called once QGIS gui is loaded, before project is loaded"""

        self.toolbar = self.iface.addToolBar("QGIS-versioning")
        self.toolbar.setVisible(True)
        
        self.info = QLabel('Versioning : no group selected')
        self.toolbar.addWidget(self.info)

        # we could have a checkbox to either replace/add layers

        self.actions.append(QAction(
            QIcon(os.path.dirname(__file__) + "/historize.svg"),
            u"historize", self.iface.mainWindow()))
        self.actions[-1].setWhatsThis("historize")
        self.actions[-1].triggered.connect(self.historize)
        self.actions[-1].setVisible(False)

        self.actions.append(QAction(
            QIcon(os.path.dirname(__file__) + "/checkout.svg"),
            u"checkout", self.iface.mainWindow()))
        self.actions[-1].setToolTip("spatialite checkout")
        self.actions[-1].setWhatsThis("checkout")
        self.actions[-1].triggered.connect(self.checkout)
        self.actions[-1].setVisible(False)

        self.actions.append(QAction(
            QIcon(os.path.dirname(__file__) + "/checkout_pg.svg"),
            u"checkout", self.iface.mainWindow()))
        self.actions[-1].setToolTip("postGIS checkout")
        self.actions[-1].setWhatsThis("checkout postgres")
        self.actions[-1].triggered.connect(self.checkout_pg)
        self.actions[-1].setVisible(False)

        self.actions.append(QAction(
            QIcon(os.path.dirname(__file__) + "/checkout_pg_local.svg"),
            u"checkout", self.iface.mainWindow()))
        self.actions[-1].setToolTip("postGIS local checkout")
        self.actions[-1].setWhatsThis("checkout local postgres")
        self.actions[-1].triggered.connect(self.checkout_pg_distant)
        self.actions[-1].setVisible(False)

        self.actions.append(QAction(
            QIcon(os.path.dirname(__file__) + "/update.svg"),
            u"update", self.iface.mainWindow()))
        self.actions[-1].setWhatsThis("update working copy")
        self.actions[-1].triggered.connect(self.update)
        self.actions[-1].setVisible(False)

        self.actions.append(QAction(
            QIcon(os.path.dirname(__file__) + "/commit.svg"),
            u"commit", self.iface.mainWindow()))
        self.actions[-1].setWhatsThis("commit modifications")
        self.actions[-1].triggered.connect(self.commit)
        self.actions[-1].setVisible(False)

        self.actions.append(QAction(
            QIcon(os.path.dirname(__file__) + "/view.svg"),
            u"view", self.iface.mainWindow()))
        self.actions[-1].setWhatsThis("see revision")
        self.actions[-1].triggered.connect(self.view)
        self.actions[-1].setVisible(False)

        self.actions.append(QAction(
            QIcon(os.path.dirname(__file__) + "/branch.svg"),
            u"branch", self.iface.mainWindow()))
        self.actions[-1].setWhatsThis("create branch")
        self.actions[-1].triggered.connect(self.branch)
        self.actions[-1].setVisible(False)
        
        self.actions.append(QAction(
            QIcon(os.path.dirname(__file__) + "/archiving.svg"),
            u"archive", self.iface.mainWindow()))
        self.actions[-1].setWhatsThis("archiving revisions")
        self.actions[-1].triggered.connect(self.archive)
        self.actions[-1].setVisible(False)

        self.actions.append(QAction(
            QIcon(os.path.dirname(__file__) + "/merge.svg"),
            u"merge", self.iface.mainWindow()))
        self.actions[-1].setWhatsThis("merge branch")
        self.actions[-1].triggered.connect(self.merge)
        self.actions[-1].setVisible(False)

        self.actions.append(QAction(
            QIcon(os.path.dirname(__file__) + "/help.svg"),
            u"help", self.iface.mainWindow()))
        self.actions[-1].setWhatsThis("versioning-help")
        self.actions[-1].setToolTip("versioning help")
        url = "http://qgis-versioning.readthedocs.org/en/latest/"
        self.actions[-1].triggered.connect(
            lambda: QDesktopServices.openUrl(QUrl(url)))
        self.actions[-1].setVisible(True)

        # add actions in menus
        for act in self.actions:
            self.toolbar.addAction(act)

    def unload(self):
        """called when plugin is unloaded"""
        # Remove the plugin menu item and icon

        self.toolbar = None
        self.actions = None
        self.info = None
        
        self.iface.layerTreeView().clicked.disconnect(self.on_legend_click)

    def __new_group(self, group_name, layers):
        """
        Create new group with given layers. Each layer is a tuple (layer uri, display name, provider)
        """

        grp = QgsProject.instance().layerTreeRoot().addGroup(group_name)
        for uri, display_name, provider in layers:
            layer = QgsVectorLayer(uri, display_name, provider)
            grp.addLayer(QgsProject.instance().addMapLayer(layer, addToLegend = False))

        return grp

    def merge(self):
        """merge branch into trunk"""
        layer = self.current_layers[0]
        uri = QgsDataSourceUri(layer.source())
        mtch = re.match(r'(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
        schema = mtch.group(1)
        base_branch = mtch.group(2)
        base_rev = mtch.group(3)

        ret = versioning.merge(self.pg_conn_info(), schema, base_branch)

        QMessageBox.information(self.iface.mainWindow(), "Merge",
                                "%d feature(s) merged." % ret)

    def branch(self):
        """create branch and import layers"""
        layer = self.current_layers[0]
        uri = QgsDataSourceUri(layer.source())
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
            QDialogButtonBox.Cancel | QDialogButtonBox.Ok)
        button_box.accepted.connect(dlg.accept)
        button_box.rejected.connect(dlg.reject)

        line_edit = QLineEdit(dlg)
        layout.addWidget(line_edit)
        layout.addWidget(button_box)
        if not dlg.exec_():
            return
        branch = line_edit.text()

        if not branch:
            print('aborted')
            return

        pcur = versioning.Db(psycopg2.connect(self.pg_conn_info()))
        pcur.execute("SELECT * FROM "+schema+".revisions "
                     "WHERE branch = '"+branch+"'")
        if pcur.fetchone():
            pcur.close()
            QMessageBox.warning(self.iface.mainWindow(), "Warning",
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
            print("aborted")
            return
        versioning.add_branch(uri.connectionInfo(),
                              schema, branch, commit_msg, base_branch, base_rev)
        layers = []
        for layer in reversed(self.current_layers):
            new_uri = QgsDataSourceUri(layer.source())
            new_uri.setDataSource(schema+'_'+branch+'_rev_head',
                                  new_uri.table(),
                                  new_uri.geometryColumn(),
                                  new_uri.sql(),
                                  new_uri.keyColumn())
            layers += [(new_uri.uri().replace('()', ''), layer.name(), 'postgres')]

        self.__new_group(branch+' revision head', layers)
            
    def view(self):
        """create view and import layers"""
        layer = self.current_layers[0]
        uri = QgsDataSourceUri(layer.source())
        mtch = re.match(r'(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
        schema = mtch.group(1)
        assert(schema)

        # Disconnect signals previously connected upon calling this function
        # The first time this function is called will throw an error because no
        # previous connections to the slots were made
        try:
            # print("Disconnecting ...")
            self.q_view_dlg.tblw.itemChanged.disconnect()
            self.q_view_dlg.diffmode_chk.stateChanged.disconnect()
        except:
            # print("Failed disconnection")
            pass

        # Make sure combobox is initalized correctly
        self.q_view_dlg.diffmode_chk.setCheckState(Qt.Unchecked)
        self.q_view_dlg.diffmode_chk.setEnabled(False)

        pcur = versioning.Db(psycopg2.connect(self.pg_conn_info()))
        pcur.execute("SELECT rev, author, date::timestamp(0), branch, commit_msg "
                     "FROM "+schema+".revisions ORDER BY rev ASC")
        revs = pcur.fetchall()
        pcur.close()

        self.q_view_dlg.tblw.setRowCount(len(revs))
        self.q_view_dlg.tblw.setColumnCount(5)
        self.q_view_dlg.tblw.setHorizontalHeaderLabels(['Rev#', 'Author', 'Date',
                                                        'Branch', 'Commit Message'])

        for i, rev in enumerate(revs):
            for j, item in enumerate(rev):
                self.q_view_dlg.tblw.setItem(i, j, QTableWidgetItem(str(item)))
                # set rev# checkable
                if j == 0:
                    self.q_view_dlg.tblw.item(i, j).setCheckState(Qt.Unchecked)

        self.q_view_dlg.tblw.itemChanged.connect(self.enable_diffmode)
        self.q_view_dlg.tblw.resizeRowsToContents()
        self.q_view_dlg.tblw.resizeColumnsToContents()
        self.q_view_dlg.diffmode_chk.stateChanged.connect(self.check_branches)

        if not self.q_view_dlg.exec_():
            return

        rows = set()
        revision_number_list = []
        branches = []

        for i in range(len(revs)):
            if self.q_view_dlg.tblw.item(i, 0).checkState():
                print("Revision " + self.q_view_dlg.tblw.item(i, 0).text() + " will be fetched")
                revision_number_list.append(
                    int(self.q_view_dlg.tblw.item(i, 0).text()))
                branches.append(self.q_view_dlg.tblw.item(i, 3).text())
                rows.add(self.q_view_dlg.tblw.item(i, 0).row())

        progressMessageBar = self.iface.messageBar().createMessage("Querying "
                                                                   "the database for revision(s) "+str(revision_number_list))
        progress = QProgressBar()
        progress.setMaximum(len(rows))
        progress.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        progressMessageBar.layout().addWidget(progress)
        self.iface.messageBar().pushWidget(progressMessageBar, Qgis.Info)
        progress.setValue(0)

        # if diffmode, create one layer with feature differences between the
        # two revisions; else checkout the full data sets for the specified
        # revisions and put them in separate layers (original behaviour)
        rev_begin = 0
        rev_end = 0
        empty_layers = []
        grp_name = ''

        if self.q_view_dlg.diffmode_chk.isChecked():
            print("Diffmode checked")
            # revision_number_list necessarily has only two items in diffmode
            rev_begin = revision_number_list[0]
            rev_end = revision_number_list[1]
            if rev_begin > rev_end:
                rev_begin, rev_end = rev_end, rev_begin
            # if the two revisions are not on the same branch, exit
            if revs[rev_begin - 1][3] != revs[rev_end - 1][3]:
                print("Revisions are not on the same branch, exiting")
                # print("Rev_begin " +  str(rev_begin) + " is on " + revs[rev_begin - 1][3])
                # print("Rev_end " + str(rev_end) + " is on " + revs[rev_end - 1][3])
                return
            else:
                print("Revisions are on the same branch")
                # print("Rev_begin " + str(rev_begin) + " is on " + revs[rev_begin - 1][3])
                # print("Rev_end " +str(rev_end) + " is on " + revs[rev_end - 1][3])

            layers = []
            for i, layer in enumerate(reversed(self.current_layers)):
                progress.setValue(i+1)
                new_uri = QgsDataSourceUri(layer.source())
                select_str = versioning.diff_rev_view_str(uri.connectionInfo(),
                                                          schema, new_uri.table(), branches[0], rev_begin, rev_end)
                # change data source uri to point to select sql
                # schema needs to be set to empty
                new_uri.setDataSource("",
                                      "("+select_str+")",
                                      new_uri.geometryColumn(),
                                      new_uri.sql(),
                                      new_uri.keyColumn())
                display_name = layer.name()
                # print("new_uri.uri() = " + new_uri.uri())
                tmp_pg_layer = QgsVectorLayer(new_uri.uri(), display_name, 'postgres')
                # print("Number of features in layer " + display_name + " = " + str(tmp_pg_layer.featureCount()))
                # if layer has no feature, delete tmp layer and resume for loop
                if not(tmp_pg_layer.featureCount()):
                    empty_layers.append(str(display_name))
                    continue

                mem_uri = self.mem_layer_uri(tmp_pg_layer)

                # print("mem_uri = " + mem_uri)
                if mem_uri == "Unknown":
                    return

                layers += [(mem_uri, display_name + '_diff', 'memory')]
                new_mem_layer = QgsVectorLayer(*layers[-1])

                pr = new_mem_layer.dataProvider()
                source_layer_features = [f for f in tmp_pg_layer.getFeatures()]
                # print("Got features from source vector layer")
                QgsProject.instance().removeMapLayer(tmp_pg_layer.id())
                # print("Removed tmp layer")
                pr.addFeatures(source_layer_features)
                # print("Copied source features to mem layer")
                # Style layer to show features as a function of whether they were
                # - added/created ('a')
                # - updated ('u')
                # - deleted ('d')
                # For all feature types, so do once
                # Code from http://snorf.net/blog/2014/03/04/symbology-of-vector-layers-in-qgis-python-plugins
                # For colors, use the names at http://www.w3schools.com/HTML/html_colornames.asp, but lowercase only; tested with "aliceblue"
                # define some rules: label, expression, color name, size, (min scale, max scale)
                modification_type_rules = (
                    ('Intermediate', '"diff_status" LIKE \'i\'',
                     'aliceblue', 2.0, None),
                    ('Created', '"diff_status" LIKE \'a\'', 'chartreuse', 3.0, None),
                    ('Updated', '"diff_status" LIKE \'u\'', 'sandybrown', 3.0, None),
                    ('Deleted', '"diff_status" LIKE \'d\'', 'red', 3.0, None),)

                symbol = QgsSymbol.defaultSymbol(
                    new_mem_layer.geometryType())
                renderer = QgsRuleBasedRenderer(symbol)
                root_rule = renderer.rootRule()
                for label, expression, color_name, size, scale in modification_type_rules:
                    # create a clone (i.e. a copy) of the default rule
                    rule = root_rule.children()[0].clone()
                    # set the label, expression and color
                    rule.setLabel(label)
                    rule.setFilterExpression(expression)
                    rule.symbol().setColor(QColor(color_name))
                    # rule.symbol().setSize(size) # works only for POINT layers
                    # set the scale limits if they have been specified
                    # if scale is not None:
                    # rule.setScaleMinDenom(scale[0])
                    # rule.setScaleMaxDenom(scale[1])
                    # append the rule to the list of rules
                    root_rule.appendChild(rule)

                # delete the default rule
                root_rule.removeChildAt(0)
                new_mem_layer.setRendererV2(renderer)
                # refresh map and legend
                self.iface.mapCanvas().refresh()

            if layers:
                grp_name = "Compare revisions " + str(rev_begin)+" vs " + str(rev_end)
                self.__new_group(grp_name, layers)
        else:
            print("Diffmode unchecked")
            layers = []
            for i, row in enumerate(rows):
                progress.setValue(i+1)
                branch = revs[row][3]
                rev = revs[row][0]
                grp_name = branch+' revision '+str(rev)
                for layer in reversed(self.current_layers):
                    new_uri = QgsDataSourceUri(layer.source())
                    select_str, where_str = versioning.rev_view_str(
                        self.pg_conn_info(),
                        schema,
                        new_uri.table(),
                        branches[0],
                        rev)
                    new_uri.setSql(where_str)
                    new_uri.setDataSource("",
                                          "("+select_str+")",
                                          new_uri.geometryColumn(),
                                          new_uri.sql(),
                                          new_uri.keyColumn())

                    src = new_uri.uri().replace('()', '')
                    layers += [(src, layer.name(), 'postgres')]
                self.__new_group(grp_name, layers)
                
        self.iface.messageBar().clearWidgets()
        # print("len (self.current_layers) = " + str(len (self.current_layers)))
        # print("len(empty_layers) = " + str(len(empty_layers)))
        if empty_layers and len(empty_layers) == len(self.current_layers):
            print("No layers in layer group")
            self.iface.messageBar().pushMessage("Notice",
                                                "No layers will be shown; deleted the \"" + grp_name + "\" layer group",
                                                level=Qgis.Warning, duration=15)
        elif empty_layers:
            print("Empty layers")
            self.iface.messageBar().pushMessage("Notice",
                                                "No modified features between revisions " +
                                                str(rev_begin)+" "
                                                "and " +
                                                str(rev_end)+" for layer(s) " +
                                                str(empty_layers)+". ",
                                                level=Qgis.Warning, duration=15)

    def unresolved_conflicts(self):
        """check for unresolved conflicts, add conflict layers if any"""
        layer = self.current_layers[0]
        uri = QgsDataSourceUri(layer.source())

        if layer.providerType() == 'spatialite':
            self.versioning = versioning.spatialite(
                uri.database(), self.pg_conn_info())
        else:
            if self.is_pgDistant(layer):
                out = self.selectDatabase()
                (pg_conn_info_out, conn_dict) = self.get_conn_from_settings(out)
                self.versioning = versioning.pgLocal(
                    pg_conn_info_out, uri.schema(), self.get_conn_from_uri(uri))
            else:
                self.versioning = versioning.pgServer(
                    self.pg_conn_info(), uri.schema())

        unresolved = self.versioning.unresolved_conflicts()
        if layer.providerType() == "spatialite":
            for cflt in unresolved:
                table = cflt+"_conflicts"
                if not QgsProject.instance().mapLayersByName(table):
                    geom = '({})'.format(uri.geometryColumn()) if uri.geometryColumn() else ''
                    self.iface.addVectorLayer(
                        "dbname=\""+uri.database()+"\"" +
                        " key=\"OGC_FID\" table=\""+table+"\" " +
                        geom, table, 'spatialite')
        else:  # postgres
            for cflt in unresolved:
                table = cflt+"_conflicts"
                if not QgsProject.instance().mapLayersByName(table):
                    new_uri = QgsDataSourceUri(uri.connectionInfo())
                    print(new_uri.uri())
                    new_uri.setDataSource(uri.schema(),
                                          table,
                                          uri.geometryColumn(),
                                          uri.sql(),
                                          uri.keyColumn())
                    src = new_uri.uri().replace('()', '')
                    self.iface.addVectorLayer(src, table, 'postgres')

        if unresolved:
            QMessageBox.warning(self.iface.mainWindow(), "Warning",
                                "Unresolved conflics for layer(s) "+', '.join(unresolved) +
                                ".\n\nPlease resolve conflicts by opening the conflict "
                                "layer attribute table, deleting either 'mine' or "
                                "'theirs' and saving before continuing.\n\n"
                                "Please note that the attribute table is not "
                                "refreshed on save (known bug), once you have deleted and "
                                "saved the unwanted change in the conflict layer, close and "
                                "reopen the attribute table to check it's empty.")
            return True
        else:
            return False

    def update(self):
        """merge modifiactions since last update into working copy"""
        print("update")
        if self.unresolved_conflicts():
            return
        layer = self.current_layers[0]
        uri = QgsDataSourceUri(layer.source())

        late_by = self.versioning.late()

        if late_by:
            self.versioning.update()
            rev = self.versioning.revision()

            # Force refresh of map
            if self.iface.mapCanvas().isCachingEnabled():
                self.iface.mapCanvas().clearCache()
                self.iface.mapCanvas().refresh()
            else:
                self.iface.mapCanvas().refresh()

            # Force refresh of rev number in menu text
            if layer.providerType() == "spatialite":
                self.info.setText(uri.database().split(
                    "/")[-1] + ' <b>working rev</b>='+str(rev))
            else:
                self.info.setText(
                    uri.database() + ' <b>working rev</b>='+str(rev))

            if not self.unresolved_conflicts():
                QMessageBox.warning(self.iface.mainWindow(), "Warning",
                                    "Working copy was late by " +
                                    str(late_by)+" revision(s).\n"
                                    "Now up to date with remote revision "+str(rev-1)+".")
        else:
            rev = self.versioning.revision()
            QMessageBox.information(self.iface.mainWindow(), "Info", "Working "
                                    "copy already up to date with remote revision "+str(rev-1)+".")

    def historize(self):
        """version database"""
        uri = None
        conn_info = ''
        schema = ''
        for layer in self.current_layers:
            uri = QgsDataSourceUri(layer.source())
            if not conn_info:
                conn_info = uri.connectionInfo()
            else:
                assert(conn_info == uri.connectionInfo())
            if not schema:
                schema = uri.schema()
            else:
                assert(schema == uri.schema())

        if QMessageBox.Ok != QMessageBox.warning(self.iface.mainWindow(),
                                                 "Warning", "This will add 4 columns to all tables in schema "
                                                 + schema +
                                                 " (i.e. even to tables not in this project)",
                                                 QMessageBox.Ok, QMessageBox.Cancel):
            print("aborted")
            return

        versioning.historize(self.pg_conn_info(), schema)

        layers = []
        for layer in reversed(self.current_layers):
            new_uri = QgsDataSourceUri(layer.source())
            new_uri.setDataSource(schema+'_trunk_rev_head',
                                  new_uri.table(),
                                  new_uri.geometryColumn(),
                                  new_uri.sql(),
                                  new_uri.keyColumn())
            src = new_uri.uri().replace('()', '')
            layers += [(src, layer.name(), 'postgres')]

        self.__new_group('trunk revision head', layers)

        self.current_group.parent().takeChild(self.current_group)
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
        for layer in self.current_layers:
            uri = QgsDataSourceUri(layer.source())

            # Get actual PK fror corresponding table
            actual_table_pk = versioning.get_actual_pk(
                uri, self.pg_conn_info())
            # print("Actual table pk = " + actual_table_pk)

            layer_selected_features_ids = [
                f[actual_table_pk] for f in layer.selectedFeatures()]

            # Check if PK from view [uri.keyColumn()] matches actual PK. If not,
            # throw error.  We need the right PK from the view in order to use
            # the efficient selectedFeaturesIds().  selectedFeatures() or other
            # ways that lead to a list of QGSFeature objects do not scale well.
            if layer_selected_features_ids:
                if uri.keyColumn() != actual_table_pk:
                    QMessageBox.warning(None, "Warning", "Layer  \""+layer.name() +
                                        " \" does not have the right primary key.\n\nCheckout will "
                                        "proceed without the selected features subset.")
                    user_selected_features.append([])
                else:
                    QMessageBox.warning(None, "Warning", "You will be checking out "
                                        "the subset of " +
                                        str(len(layer_selected_features_ids)
                                            )+" features "
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
            table = uri.schema()+"."+uri.table()
            tables_for_conninfo.append(table)

        filename, _ = QFileDialog.getSaveFileName(self.iface.mainWindow(),
                                                  'Save Versioned Layers As', '.', '*.sqlite')

        if not filename:
            print("aborted")
            return

        if os.path.isfile(filename):
            os.remove(filename)

        print("checking out ", tables_for_conninfo, " from ", uri.connectionInfo())
        self.versioning = versioning.spatialite(filename, self.pg_conn_info())
        self.versioning.checkout(tables_for_conninfo, user_selected_features)

        # add layers from offline version
        grp_name = 'working copy'
        if grp_name in QgsProject.instance().mapLayers():
            grp_name = filename

        layers = []
        for layer in reversed(self.current_layers):
            uri = QgsDataSourceUri(layer.source())
            table = uri.table()
            display_name = layer.name()
            geom = '({})'.format(uri.geometryColumn()) if uri.geometryColumn() else ''
            layers += [("dbname=\""+filename+"\"" +
                       " key=\"OGC_FID\" table=\""+table+"_view\" "
                       + geom, display_name, 'spatialite')]

        self.__new_group(grp_name, layers).setExpanded(True)

    def checkout_pg_distant(self):
        """create postgres working copy (schema) from versioned
        database layers"""
        # for each connection, we need the list of tables
        tables_for_conninfo = []
        # for each layer, we need the list of user selected features to be
        # checked out; if a given layer has no user selected features, then all
        # features will be checked out
        user_selected_features = []
        uri = None
        conn_info = ''
        for layer in self.current_layers:
            uri = QgsDataSourceUri(layer.source())

            # Get actual PK fror corresponding table
            actual_table_pk = versioning.get_actual_pk(
                uri, self.pg_conn_info())
            # print("Actual table pk = " + actual_table_pk)

            layer_selected_features_ids = [
                f[actual_table_pk] for f in layer.selectedFeatures()]

            # Check if PK from view [uri.keyColumn()] matches actual PK. If not,
            # throw error.  We need the right PK from the view in order to use
            # the efficient selectedFeaturesIds().  selectedFeatures() or other
            # ways that lead to a list of QGSFeature objects do not scale well.
            if layer_selected_features_ids:
                if uri.keyColumn() != actual_table_pk:
                    QMessageBox.warning(None, "Warning", "Layer  \""+layer.name() +
                                        " \" does not have the right primary key.\n\nCheckout will "
                                        "proceed without the selected features subset.")
                    user_selected_features.append([])
                else:
                    QMessageBox.warning(None, "Warning", "You will be checking out "
                                        "the subset of " +
                                        str(len(layer_selected_features_ids)
                                            )+" features "
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
            table = uri.schema()+"."+uri.table()
            tables_for_conninfo.append(table)

        # DBase checkout
        exportDatabase = self.selectDatabase()
        if not exportDatabase:
            return

        layer = self.current_layers[0]
        uri = QgsDataSourceUri(layer.source())

        pg_conn_info = self.get_conn_from_uri(uri)

        # TODO: normally a test on uri.schema should be sufficent
        schema = uri.schema()
        schemaShort = schema[:-len("_trunk_rev_head")]

        # check if schema already exists
        (pg_conn_info_out, conn_dict) = self.get_conn_from_settings(exportDatabase)
        conn = psycopg2.connect(pg_conn_info_out)
        cur = conn.cursor()

        cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name in ('{}', '{}');".format(
            schema, schemaShort))
        a = cur.fetchall()
        if len(a) > 0:
            QMessageBox.critical(self.iface.mainWindow(),
                                 "Error",
                                 "Schema {} already exists in database".format(
                                     schema),
                                 QMessageBox.Close)
            return

        working_copy_schema = schema
        self.versioning = versioning.pgLocal(
            self.pg_conn_info(), working_copy_schema, pg_conn_info_out)
        self.versioning.checkout(tables_for_conninfo, user_selected_features)

        # add layers from offline version
        layers = []
        for layer in reversed(self.current_layers):
            new_uri = QgsDataSourceUri(layer.source())
            new_uri.setDataSource(working_copy_schema,
                                  new_uri.table()+"_view",
                                  'geom',
                                  new_uri.sql(),
                                  new_uri.keyColumn())
            # TODO: IT'S UGLY
            if 'service' in conn_dict:
                new_uri = QgsDataSourceUri(' '.join(["service="+conn_dict['service'],
                                                     "key='ogc_fid'",
                                                     new_uri.uri()[new_uri.uri().rfind('srid'):]]))
            else:
                new_uri = QgsDataSourceUri(' '.join(["dbname="+conn_dict['database'],
                                                     "host="+conn_dict['host'],
                                                     "port="+conn_dict['port'],
                                                     "user='" +
                                                     conn_dict['username']+"'",
                                                     "password='" +
                                                     conn_dict['password']+"'",
                                                     "key='ogc_fid'",
                                                     new_uri.uri()[new_uri.uri().rfind('srid'):]]))

            display_name = layer.name()
            src = new_uri.uri().replace('()', '')
            layers += [(src, display_name, 'postgres')]

        self.__new_group(working_copy_schema, layers)
        
    def checkout_pg(self):
        """create postgres working copy (schema) from versioned
        database layers"""
        # for each connection, we need the list of tables
        tables_for_conninfo = []
        user_selected_features = []
        uri = None
        conn_info = ''
        for layer in self.current_layers:
            uri = QgsDataSourceUri(layer.source())

            # Get actual PK fror corresponding table
            actual_table_pk = versioning.get_actual_pk(
                uri, self.pg_conn_info())
            # print("Actual table pk = " + actual_table_pk)

            layer_selected_features_ids = [
                f[actual_table_pk] for f in layer.selectedFeatures()]

            # Check if PK from view [uri.keyColumn()] matches actual PK. If not,
            # throw error.  We need the right PK from the view in order to use
            # the efficient selectedFeaturesIds().  selectedFeatures() or other
            # ways that lead to a list of QGSFeature objects do not scale well.
            if layer_selected_features_ids:
                if uri.keyColumn() != actual_table_pk:
                    QMessageBox.warning(None, "Warning", "Layer  \""+layer.name() +
                                        " \" does not have the right primary key.\n\nCheckout will "
                                        "proceed without the selected features subset.")
                    user_selected_features.append([])
                else:
                    QMessageBox.warning(None, "Warning", "You will be checking out "
                                        "the subset of " +
                                        str(len(layer_selected_features_ids)
                                            )+" features "
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
            table = uri.schema()+"."+uri.table()
            tables_for_conninfo.append(table)

        dlg = QDialog()
        dlg.setWindowTitle('Enter working copy schema name')
        layout = QVBoxLayout(dlg)
        button_box = QDialogButtonBox(dlg)
        button_box.setStandardButtons(
            QDialogButtonBox.Cancel | QDialogButtonBox.Ok)
        button_box.accepted.connect(dlg.accept)
        button_box.rejected.connect(dlg.reject)

        line_edit = QLineEdit(dlg)
        layout.addWidget(line_edit)
        layout.addWidget(button_box)
        if not dlg.exec_():
            return
        working_copy_schema = line_edit.text()
        if not working_copy_schema:
            print("Name not provided; aborted")
            self.iface.messageBar().pushMessage("Warning",
                                                "Please provide a schema name.", duration=5)
            return
        # Check if name is valid for a PG object; only characters and max length
        # are checked; use of reserved words is not checked
        if len(working_copy_schema) > 63:
            print("Name too long; aborted")
            self.iface.messageBar().pushMessage("Warning",
                                                "\""+working_copy_schema+"\" is "+str(len(working_copy_schema)) +
                                                " characters long;  maximum is 63.", duration=5)
            return
        valid_name = re.match('^[a-z_][a-z_0-9$]*$', str(working_copy_schema))
        if not(valid_name):
            print("Not a valid name")
            self.iface.messageBar().pushMessage("Warning",
                                                "\""+working_copy_schema+"\" is not valid; first character must be "
                                                "<b>lowercase</b> letter or underscore; other characters may be "
                                                "<b>lowercase</b> letters, underscore or digits.", duration=10)
            return
        print("checking out ", tables_for_conninfo, " from ", uri.connectionInfo())
        self.versioning = versioning.pgServer(
            self.pg_conn_info(), working_copy_schema)
        self.versioning.checkout(tables_for_conninfo, user_selected_features)

        # add layers from offline version
        layers = []
        for layer in reversed(self.current_layers):
            new_uri = QgsDataSourceUri(layer.source())
            new_uri.setDataSource(working_copy_schema,
                                  new_uri.table()+"_view",
                                  new_uri.geometryColumn(),
                                  new_uri.sql(),
                                  new_uri.keyColumn())
            display_name = layer.name()
            print("replacing ", display_name)
            src = new_uri.uri().replace('()', '')
            layers += [(src, display_name, 'postgres')]
        self.__new_group(working_copy_schema, layers)

    def commit(self):
        """merge modifications into database"""
        print("commit")
        if self.unresolved_conflicts():
            return

        layer = self.current_layers[0]
        uri = QgsDataSourceUri(layer.source())

        late_by = self.versioning.late()

        if late_by:
            QMessageBox.warning(self.iface.mainWindow(), "Warning",
                                "This working copy is not up to date (late by "
                                + str(late_by)+" commit(s)).\n\n"
                                "Please update before committing your modifications")
            print("aborted")
            return

        # Make sure the combobox is visible; could be made invisible by a
        # previous call to branch
        self.q_commit_msg_dlg.pg_users_combobox.setVisible(True)
        self.q_commit_msg_dlg.pg_username_label.setVisible(True)
        # Populate combobox with list of pg usernames
        nb_items_in_list = self.q_commit_msg_dlg.pg_users_combobox.count()
        if not(nb_items_in_list):
            self.get_pg_users_list()
        # Better if we could have a QgsDataSourceUri.username() but no such
        # thing in spatialite.  Next block is for the case the username cannot
        # be found in the connection info string (mainly for plugin tests)
        try:
            pg_username = self.pg_conn_info().split(
                ' ')[3].replace("'", "").split('=')[1]
            current_user_index = self.q_commit_msg_dlg.pg_users_combobox.findText(
                pg_username)
            # sets the current pg_user in the combobox to come
            current_user_combobox_item = self.q_commit_msg_dlg.pg_users_combobox.setCurrentIndex(
                current_user_index)
        except (IndexError):
            pg_username = ''

        # time to get the commit message
        if not self.q_commit_msg_dlg.exec_():
            return
        commit_msg = self.q_commit_msg_dlg.commitMessage.document().toPlainText()
        commit_pg_user = self.q_commit_msg_dlg.pg_users_combobox.itemText(
            self.q_commit_msg_dlg.pg_users_combobox.currentIndex())

        if not commit_msg:
            QMessageBox.warning(self.iface.mainWindow(), "Warning",
                                "No commit message, aborting commit")
            print("aborted")
            return

        nb_of_updated_layer = 0
        rev = 0
        nb_of_updated_layer = self.versioning.commit(
            commit_msg, commit_pg_user)
        rev = self.versioning.revision()

        if nb_of_updated_layer:
            # self.iface.messageBar().pushMessage("Info",
            # "You have successfully committed revision "+str( rev ), duration=10)
            QMessageBox.information(self.iface.mainWindow(), "Info",
                                    "You have successfully committed remote revision "+str(rev-1))

            # Force refresh of rev number in menu text
            if layer.providerType() == "spatialite":
                self.info.setText(uri.database().split(
                    "/")[-1] + ' <b>working rev</b>='+str(rev))
            else:
                self.info.setText(
                    uri.database() + ' <b>working rev</b>='+str(rev))
        else:
             # self.iface.messageBar().pushMessage("Info",
            # "There was no modification to commit", duration=10)
            QMessageBox.information(self.iface.mainWindow(), "Info",
                                    "There was no modification to commit")

    def archive(self):
        layer = self.current_layers[0]
        uri = QgsDataSourceUri(layer.source())
        mtch = re.match(r'(.+)_([^_]+)_rev_(head|\d+)', uri.schema())
        schema = mtch.group(1)
        base_branch = mtch.group(2)
        base_rev = mtch.group(3)
        
        dlg = QDialog()
        dlg.setWindowTitle('Choose the revisions to archiving')
        layout = QVBoxLayout(dlg)
        button_box = QDialogButtonBox(dlg)
        button_box.setStandardButtons(
            QDialogButtonBox.Cancel | QDialogButtonBox.Ok)
        button_box.accepted.connect(dlg.accept)
        button_box.rejected.connect(dlg.reject)
    
        label = QLabel(dlg)
        label.setText('Archiving until revision number: ')
        revBox = QComboBox(dlg)
        
        
        pcur = versioning.Db(psycopg2.connect(self.pg_conn_info()))
    
        pcur.execute("SELECT MAX(rev) FROM "+schema+".revisions")
        [max_rev] = pcur.fetchone()
        
        revisions = [str(rev) for rev in range(1, max_rev+1)]
        revBox.addItems(revisions)
    
        layout.addWidget(label)
        layout.addWidget(revBox)
        layout.addWidget(button_box)
        if not dlg.exec_():
            return None
    
        versioning.archive(self.pg_conn_info(), schema, revBox.currentText())
