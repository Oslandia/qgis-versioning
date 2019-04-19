# -*- coding: utf-8 -*-

import os
import sys

# So we can use absolute path
path_root = os.path.join(os.path.dirname(__file__))
sys.path.append(path_root)


def classFactory(iface):
    from .plugin import Plugin
    return Plugin(iface)
