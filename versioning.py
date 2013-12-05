
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
        print "Versioning.work_offline"
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
                    database = uri.database()
                    # use ogr2ogr to create spatialite db
                    if not filename:
                        filename = QFileDialog.getSaveFileName(self.iface.mainWindow(), 'Save Versionned Layers As', '.', '*.sqlite')
                        cmd = "ogr2ogr -preserve_fid -f SQLite -dsco SPATIALITE=yes "+filename+" PG:\"dbname='"+database+"' active_schema="+schema+"\" "+table
                        print cmd
                        os.remove(filename)
                        os.system(cmd)
                    else:
                        cmd = "ogr2ogr -preserve_fid -f SQLite -update "+filename+" PG:\"dbname='"+database+"' active_schema="+schema+"\" "+table
                        print cmd
                        os.system(cmd)
                    # replace layer by it's offline version
                    registry.removeMapLayer(name)
                    self.iface.addVectorLayer("dbname="+filename+" table=\""+table+"\" (GEOMETRY)\"",table,'spatialite')



        
        pass

    def commit(self):
        print "Versioning.commit"
        pass
