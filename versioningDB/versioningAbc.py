# -*- coding: utf-8 -*-

from __future__ import absolute_import
import abc

class AbstractVersioning(object):
    __metaclass__ = abc.ABCMeta
    
#    def __init__(self):
#        pass
        
    @abc.abstractmethod
    def revision( connection ):
        return
    
    @abc.abstractmethod
    def late( connection ):
        return
    
    @abc.abstractmethod
    def update(connection):
        return
    
    @abc.abstractmethod
    def checkout(pg_conn_info, pg_table_names, working_source, selected_feature_lists = []):
        return
    
    @abc.abstractmethod
    def unresolved_conflicts(connection):
        return
    
    @abc.abstractmethod
    def commit(connection, commit_msg, commit_user = ''):
        return
        