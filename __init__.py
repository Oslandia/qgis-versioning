# -*- coding: utf-8 -*-

from __future__ import absolute_import

def classFactory(iface):
    from .plugin import Plugin
    return Plugin(iface)
