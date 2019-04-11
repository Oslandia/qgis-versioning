#!/usr/bin/env python3

from PyQt5.QtWidgets import QMessageBox
from qgis.core import (QgsApplication, QgsVectorLayer, QgsProject)
import sys
import os
import plugin

dbname = "epanet_test_db"
wc_dbname = "epanet_test_wc_db"
schema = "epanet"
wcs = "epanet_wc"
test_data_dir = os.path.dirname(os.path.realpath(__file__))
sql_file = os.path.join(test_data_dir, "epanet_test_db.sql")

# Monkey path GUI stuff
# This not ideal to monkey patch too much things. It will be better to put
# most of the GUI things in methods and to monkey patch these methods like
# it's done with selectDatabase


class EmptyObject(object):
    def __getattr__(self, name):
        return EmptyObject()

    def __call__(self, *args):
        return EmptyObject()


def generate_tempfile(*args):
    return ("/tmp/plugin_test_file.sqlite", None)


def return_ok(*args):
    return QMessageBox.Ok


class QLineEdit:

    def __init__(*args):
        pass

    def text(self):
        return wcs


def return_wc_database():
    return wc_dbname


iface = EmptyObject()
iface.mainWindow = EmptyObject()
iface.layerTreeView = EmptyObject()
plugin.QDialog = EmptyObject()
plugin.uic.loadUi = EmptyObject()
plugin.QFileDialog.getSaveFileName = generate_tempfile
plugin.QMessageBox.warning = return_ok
plugin.QVBoxLayout = EmptyObject()
plugin.QDialogButtonBox = EmptyObject()
plugin.QDialogButtonBox.Cancel = 0
plugin.QDialogButtonBox.Ok = 0
plugin.QLineEdit = QLineEdit


class PluginTest:

    def __init__(self, host, pguser):

        # create the test database
        os.system(f"dropdb --if-exists -h {host} -U {pguser} {dbname}")
        os.system(f"createdb -h {host} -U {pguser} {dbname}")
        os.system(f"psql -h {host} -U {pguser} {dbname} -f {sql_file}")

        # Initialize project
        layer_source = f"""dbname='{dbname}' user='{pguser}'
        table="epanet"."junctions" (geom) sql="""
        j_layer = QgsVectorLayer(layer_source, "junctions", "postgres")
        assert(j_layer and j_layer.isValid() and j_layer.featureCount() == 2)
        assert(QgsProject.instance().addMapLayer(j_layer, False))

        root = QgsProject.instance().layerTreeRoot()
        group = root.addGroup("epanet_group")
        group.addLayer(j_layer)

        self.versioning_plugin = plugin.Plugin(iface)
        self.versioning_plugin.current_layers = [j_layer]
        self.versioning_plugin.current_group = group

    def test_historize_checkout(self):

        root = QgsProject.instance().layerTreeRoot()

        # historize
        self.versioning_plugin.historize()
        assert(len(root.children()) == 1)
        group = root.children()[0]
        assert(group.name() == "trunk revision head")
        j_layer = group.children()[0].layer()
        assert(j_layer.name() == "junctions")

        self.versioning_plugin.current_layers = [j_layer]
        self.versioning_plugin.current_group = group

        # checkout
        self.checkout()
        assert(len(root.children()) == 2)
        group = root.children()[1]
        assert(group.name() == self.get_working_name())

    def __del__(self):
        QgsProject.instance().clear()

    def checkout(self):
        raise Exception("Must be overrided")

    def get_working_name(self):
        raise Exception("Must be overrided")


class SpatialitePluginTest(PluginTest):

    def __init__(self, host, pguser):
        super().__init__(host, pguser)

    def checkout(self):
        self.versioning_plugin.checkout()

    def get_working_name(self):
        return "working copy"


class PgServerPluginTest(PluginTest):

    def __init__(self, host, pguser):
        super().__init__(host, pguser)

    def checkout(self):
        self.versioning_plugin.checkout_pg()

    def get_working_name(self):
        return wcs


class PgLocalPluginTest(PluginTest):

    def __init__(self, host, pguser):

        super().__init__(host, pguser)

        # create the test database
        os.system(f"dropdb --if-exists -h {host} -U {pguser} {wc_dbname}")
        os.system(f"createdb -h {host} -U {pguser} {wc_dbname}")

        # Monkey patch the GUI to return database name
        self.versioning_plugin.selectDatabase = return_wc_database

    def checkout(self):
        self.versioning_plugin.checkout_pg_distant()

    def get_working_name(self):
        return "epanet_trunk_rev_head"


def test(host, pguser):

    qgs = QgsApplication([], False)
    qgs.initQgis()

    for test_class in [SpatialitePluginTest,
                       PgLocalPluginTest,
                       PgServerPluginTest]:

        test = test_class(host, pguser)
        test.test_historize_checkout()
        del test

    qgs.exitQgis()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 versioning_base_test.py host pguser")
    else:
        test(*sys.argv[1:])
