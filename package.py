#!/usr/bin/python
# coding=UTF-8
"""
packaging script for the qgis_versioning project

USAGE
    python -m qgispackage.py [-h, -i, -u] [directory],

OPTIONS
    -h, --help
        print this help

    -i, --install [directory]
        install the package in the .qgis2 directory, if directory is ommited, 
        install in the QGis plugin directory

    -u, --uninstall
        uninstall (remove) the package from .qgis2 directory
"""

import os
import zipfile
import re
import shutil

# @todo make that work on windows
qgis_plugin_dir = os.path.join(os.path.expanduser('~'), ".qgis2", "python", "plugins")

def uninstall(install_dir):
    target_dir = os.path.join(install_dir, "qgis_versioning")
    if os.path.isdir(target_dir):
        shutil.rmtree(target_dir)

def install(install_dir, zip_filename):
    uninstall(install_dir)
    with zipfile.ZipFile(zip_filename, "r") as z:
        z.extractall(install_dir)
    print "installed in", install_dir

def zip_(zip_filename):
    """the zip file doesn't include tests, demos or doc"""
    qgis_versioning_dir = os.path.dirname(__file__)
    with zipfile.ZipFile(zip_filename, 'w') as package:
        for root, dirs, files in os.walk(qgis_versioning_dir):
            if not re.match(r".*(test_data|doc|tmp).*", root):
                for file_ in files:
                    if re.match(r".*\.(py|txt|ui|svg|png|insat|sat|qml|sql)$", file_) \
                            and not re.match(r".*(_test|_demo)\.py", file_) \
                            and not re.match(r"(package.py|test.py)", file_):
                        fake_root = root.replace(qgis_versioning_dir, "qgis_versioning")
                        package.write(os.path.join(root, file_), 
                                      os.path.join(fake_root, file_))


if __name__ == "__main__":
    import getopt
    import sys

    try:
        optlist, args = getopt.getopt(sys.argv[1:],
                "hiu",
                ["help", "install", "uninstall"])
    except Exception as e:
        sys.stderr.write(str(e)+"\n")
        exit(1)

    optlist = dict(optlist)

    if "-h" in optlist or "--help" in optlist:
        help(sys.modules[__name__])
        exit(0)

    zip_filename = os.path.join(os.path.dirname(__file__), "qgis_versioning.zip")
    zip_(zip_filename)
    install_dir = qgis_plugin_dir if len(args)==0 else args[0]

    if "-u" in optlist or "--uninstall" in optlist:
        uninstall(install_dir)

    if "-i" in optlist or "--install" in optlist:
        install(install_dir, zip_filename)
        
