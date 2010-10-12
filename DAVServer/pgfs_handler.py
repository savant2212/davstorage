import sqlite3
# To change this template, choose Tools | Templates
# and open the template in the editor.

import sys
import urlparse
import os
import time
from string import joinfields, split, lower
import logging
import types
import shutil
from DB import *
import sqlalchemy


from DAV.constants import COLLECTION, OBJECT
from DAV.errors import *
from DAV.iface import *

from DAV.davcmd import copyone, copytree, moveone, movetree, delone, deltree

log = logging.getLogger(__name__)

BUFFER_SIZE = 128 * 1000
# include magic support to correctly determine mimetypes
MAGIC_AVAILABLE = False
try:
    import mimetypes
    MAGIC_AVAILABLE = True
except ImportError:
    pass


class FilesystemHandler(dav_interface):

    def __init__(self, prefix, conn_string):
        self.Prefix=prefix
        engine = create_engine(conn_string, echo=True)
        return

    def get_childs(self, uri):
        filelist = []

        dir_id=uri2id(uri)

        return filelist

    def uri2id(self, uri):
        """Documentation"""
        uparts=urlparse.urlparse(uri)
        loc=uparts[2][1:]

        if loc == Prefix:
            return 1

        path = loc[loc.find(self.Prefix)+len(self.Prefix):]
        db.row_factory = sqlite3.Row

        cur = db.cursor() # создание курсора
        cur.execute("select * from directories where Name = '" + path + "'")
        for row in cur:
            print row


        return filename

