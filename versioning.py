
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
import os.path

qset = QSettings( "oslandia", "horao_qgis_plugin" )

WIN_TITLE = "versioning"

class Versioning:
    def __init__(self, iface):
        # Save reference to the QGIS interface
        self.iface = iface
        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)
        # map of layer object => LayerInfo
        self.layers = {}

    def initGui(self):
        # Create action  work_offline
        self.work_offline = QAction(
            QIcon(":/plugins/versioning/work_offline.svg"),
            u"work offline", self.iface.mainWindow())
        # connect the action to the run method
        self.work_offline.triggered.connect(self.test1)

        # Add toolbar button and menu item
        self.iface.addToolBarIcon(self.work_offline)
        self.iface.addPluginToMenu( WIN_TITLE, self.work_offline)

        # Create action commit
        self.commit = QAction(
            QIcon(":/plugins/versioning/commit.svg"),
            u"work offline", self.iface.mainWindow())
        # connect the action to the run method
        self.commit.triggered.connect(self.test1)

        # Add toolbar button and menu item
        self.iface.addToolBarIcon(self.commit)
        self.iface.addPluginToMenu( WIN_TITLE, self.commit)

    def unload(self):
        # Remove the plugin menu item and icon
        self.iface.removePluginMenu( WIN_TITLE, self.action)
        self.iface.removeToolBarIcon(self.work_offline)
        self.iface.removeToolBarIcon(self.commit)

    def test1(self):
        print "Versioning.test1"
        pass

    def test2(self):
        print "Versioning.test2"
        pass
