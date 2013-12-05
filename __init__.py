# -*- coding: utf-8 -*-

def classFactory(iface):
    # load Canvas3D class from file Canvas3D
    from versioning import *
    return Versioning(iface)
