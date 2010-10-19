#Copyright (c) 2010 Nazarenko Nikita (god@savant.su)
#
#This library is free software; you can redistribute it and/or
#modify it under the terms of the GNU Library General Public
#License as published by the Free Software Foundation; either
#version 2 of the License, or (at your option) any later version.
#
#This library is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#Library General Public License for more details.
#
#You should have received a copy of the GNU Library General Public
#License along with this library; if not, write to the Free
#Software Foundation, Inc., 59 Temple Place - Suite 330, Boston,
#MA 02111-1307, USA

from Entity.Content import Content
from Entity.TreeObject import TreeObject
import sys
import urlparse
import os
import time
from string import joinfields, split, lower
import logging
import types
import shutil

from DAV.constants import COLLECTION, OBJECT
from DAV.errors import *
from DAV.iface import *

from DAV.davcmd import copyone, copytree, moveone, movetree, delone, deltree
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper, sessionmaker

log = logging.getLogger(__name__)

BUFFER_SIZE = 128 * 1000 
# include magic support to correctly determine mimetypes
MAGIC_AVAILABLE = False
try:
    import mimetypes
    MAGIC_AVAILABLE = True
except ImportError:
    pass

class Resource(object):
    # XXX this class is ugly
    def __init__(self, fp, file_size):
        self.__fp = fp
        self.__file_size = file_size

    def __len__(self):
        return self.__file_size

    def __iter__(self):
        while 1:
            data = self.__fp.read(BUFFER_SIZE)
            if not data:
                break
            yield data
            time.sleep(0.005)
        self.__fp.close()

    def read(self, length = 0):
        if length == 0:
            length = self.__file_size

        data = self.__fp.read(length)
        return data
        

class DBFSHandler(dav_interface):
    """ 
    Model a filesystem for DAV

    This class models a regular filesystem for the DAV server

    The basic URL will be http://localhost/
    And the underlying filesystem will be /tmp

    Thus http://localhost/gfx/pix will lead
    to /tmp/gfx/pix

    """
    Base = declarative_base()

    def __init__(self, connection_string, uri, verbose=False):
        self.setEngine(connection_string)
        self.setBaseURI(uri)
        # should we be verbose?
        self.verbose = verbose
        log.info('Initialized with %s' % (uri))

    def setEngine(self, connection_string):
        """ Set sqlalchemy engine"""
        self.metadata = Base.metadata
        self.engine = create_engine(connection_string, echo=True)
        
        self.session = sessionmaker(bind=self.engine)
        root_element = session.query(TreeOblect).filter_by(id='1').first()

        if root_element == null :
            root_element=TreeObject("/",1,null,0,0,0,0)
            self.session.add(root_element)

    def setBaseURI(self, uri):
        """ Sets the base uri """

        self.baseuri = uri

    def uri2id(self,uri):
        """ map uri in baseuri and local part """
        raise NotImplementedError
        uparts=urlparse.urlparse(uri)
        fileloc=uparts[2][1:]

        #get object id

        return id

    def object2uri(self,filename):
        """ map local filename to self.baseuri """
        raise NotImplementedError
        
        uri=urlparse.urljoin(self.baseuri,sparts)
        return uri


    def get_childs(self,uri):
        """ return the child objects as self.baseuris for the given URI """
        raise NotImplementedError
        
    def get_data(self,uri, range = None):
        """ return the content of an object """
        raise NotImplementedError

    def _get_dav_resourcetype(self,uri):
        """ return type of object """
        raise NotImplementedError
        path=self.uri2local(uri)
        if os.path.isfile(path):
            return OBJECT

        elif os.path.isdir(path):
            return COLLECTION

        raise DAV_NotFound

    def _get_dav_displayname(self,uri):
        raise NotImplementedError

    def _get_dav_getcontentlength(self,uri):
        """ return the content length of an object """
        raise NotImplementedError

    def get_lastmodified(self,uri):
        """ return the last modified date of the object """
        raise NotImplementedError
        

    def get_creationdate(self,uri):
        """ return the last modified date of the object """
        raise NotImplementedError

    def _get_dav_getcontenttype(self,uri):
        """ find out yourself! """

        raise NotImplementedError

    def put(self, uri, data, content_type=None):
        """ put the object into the filesystem """
        raise NotImplementedError
        

    def mkcol(self,uri):
        """ create a new collection """
        raise NotImplementedError

    ### ?? should we do the handler stuff for DELETE, too ?
    ### (see below)

    def rmcol(self,uri):
        """ delete a collection """
        raise NotImplementedError

    def rm(self,uri):
        """ delete a normal resource """
        raise NotImplementedError

    ###
    ### DELETE handlers (examples)
    ### (we use the predefined methods in davcmd instead of doing
    ### a rm directly
    ###

    def delone(self,uri):
        """ delete a single resource

        You have to return a result dict of the form
        uri:error_code
        or None if everything's ok

        """
        raise NotImplementedError

    def deltree(self,uri):
        """ delete a collection 

        You have to return a result dict of the form
        uri:error_code
        or None if everything's ok
        """
        raise NotImplementedError
        


    ###
    ### MOVE handlers (examples)
    ###

    def moveone(self,src,dst,overwrite):
        """ move one resource with Depth=0
        """

        return moveone(self,src,dst,overwrite)

    def movetree(self,src,dst,overwrite):
        """ move a collection with Depth=infinity
        """

        return movetree(self,src,dst,overwrite)

    ###
    ### COPY handlers
    ###

    def copyone(self,src,dst,overwrite):
        """ copy one resource with Depth=0
        """

        return copyone(self,src,dst,overwrite)

    def copytree(self,src,dst,overwrite):
        """ copy a collection with Depth=infinity
        """

        return copytree(self,src,dst,overwrite)

    ###
    ### copy methods.
    ### This methods actually copy something. low-level
    ### They are called by the davcmd utility functions
    ### copytree and copyone (not the above!)
    ### Look in davcmd.py for further details.
    ###

    def copy(self,src,dst):
        """ copy a resource from src to dst """

        raise NotImplementedError

    def copycol(self, src, dst):
        """ copy a collection.

        As this is not recursive (the davserver recurses itself)
        we will only create a new directory here. For some more
        advanced systems we might also have to copy properties from
        the source to the destination.
        """

        return self.mkcol(dst)

    def exists(self,uri):
        """ test if a resource exists """
        raise NotImplementedError

    def is_collection(self,uri):
        """ test if the given uri is a collection """
        raise NotImplementedError
