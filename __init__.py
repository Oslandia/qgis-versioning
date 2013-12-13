# -*- coding: utf-8 -*-

def classFactory(iface):
    # load Canvas3D class from file Canvas3D
    import versioning
    return versioning.Versioning(iface)
