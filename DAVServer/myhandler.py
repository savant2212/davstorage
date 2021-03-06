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

from DAV.errors import DAV_Error
from DAV.errors import DAV_NotFound
from logging import debug
import sys
import urlparse
import os, string
import time
from string import joinfields, split, lower
import logging
import types
import shutil
import time
import base64

from DAV.constants import COLLECTION, OBJECT
from DAV.errors import *
from DAV.iface import *

from DAV.davcmd import copyone, copytree, moveone, movetree, delone, deltree

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, DateTime, Float
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

Base = declarative_base()

class Content(Base):
    __tablename__= 'Contents'

    id       = Column(Integer, primary_key=True)
    object_id = Column(Integer)
    revision = Column(Integer)
    content  = Column(String)
    mod_time = Column(Float)


    def __init__(self, revision, content, tree_object, mod_time=time.time()):
        self.revision   = revision
        self.content    = content
        self.object_id  = tree_object
        mod_time        = mod_time

    def __repr__(self):
        return "<Content('%s','%s', '%s')>" % (self.tree_object, self.content, self.revision)


class TreeObject(Base):
    __tablename__= 'TreeObjects'
    id      = Column(Integer, primary_key=True)
    name    = Column(String)
    type    = Column(Integer)
    parent  = Column(Integer)
    owner   = Column(Integer)
    group   = Column(Integer)
    size    = Column(Integer)    
    path    = Column(String)
    mod_time = Column(Float)
    creat_time = Column(Float)

    def __init__(self, name, type, parent, owner, group, size, content, path,
        creat_time=time.time(), mod_time=time.time()):
         self.name      = name
         self.type      = type
         self.parent    = parent
         self.owner     = owner
         self.group     = group
         self.size      = size
         self.content   = content
         self.path      = path
         self.mod_time  = mod_time
         self.creat_time= creat_time

    def __repr__(self):
             return "<TreeObject('%s','%s','%s','%s','%s','%s','%s', '%s')>" % (
         self.name, self.type, self.parent, self.owner, self.group, self.size,
         self.content, self.path)

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

    def setup(self):
        """Documentation"""
        sess = self.Session()
        root_element = sess.query(TreeObject).filter_by(id='1').first()

        if root_element == None :
            root_element=TreeObject("/",1,None,0,0,0,0,'/')
            sess.add(root_element)
            sess.commit()
            sess.close()

    def setEngine(self, connection_string):
        """ Set sqlalchemy engine"""
        
        self.metadata = Base.metadata
        self.engine = create_engine(connection_string, echo=True)

        print(self.metadata)

        self.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        
        

    def setBaseURI(self, uri):
        """ Sets the base uri """

        self.baseuri = uri
        
    # @type obj TreeObject
    def uri2obj(self,uri):
        """ map uri in baseuri and local part """
        sess = self.Session()

        uparts=urlparse.urlparse(uri)
        fileloc=uparts[2]
        print('fileloc: %s' % (fileloc) )
        #get object id
        element=None
        if fileloc != '':
            element = sess.query(TreeObject).filter_by(path=fileloc).first()
        else :
            element = sess.query(TreeObject).filter_by(id='1').first()

        sess.close()

        if element == None:
            return None

        return element

    def object2uri(self,obj):
        """ map local filename to self.baseuri """
        uri=urlparse.urljoin(self.baseuri,obj.path)
        return uri


    def get_childs(self,uri):
        """ return the child objects as self.baseuris for the given URI """
        # @type obj TreeObject
        obj=self.uri2obj(uri)
        sess = self.Session()
        filelist = []
        # @type elt TreeObject
        for elt in sess.query(TreeObject).filter_by(parent=obj.id).order_by(TreeObject.name):
            print(elt.name)
            filelist.append(self.object2uri(elt))

        return filelist

    def get_data(self,uri, range = None):
        """ return the content of an object """
        obj=self.uri2obj(uri)
        #if obj.type == 0:

        raise DAV_Error

    def _get_dav_resourcetype(self,uri):
        """ return type of object """        
        obj=self.uri2obj(uri)
        if obj.type == 0:
            return OBJECT
        elif  obj.type == 1:
            return COLLECTION

        raise DAV_NotFound

    def _get_dav_displayname(self,uri):

        obj = self.uri2obj(uri)
        # @type obj TreeObject
        return obj.name

    def _get_dav_getcontentlength(self,uri):
        """ return the content length of an object """
        return '0'

    def get_lastmodified(self,uri):
        """ return the last modified date of the object """
        # @type obj TreeObject
        obj = self.uri2obj(uri)

        return obj.mod_time
        

    def get_creationdate(self,uri):
        """ return the creation time of the object """
        # @type obj TreeObject
        obj = self.uri2obj(uri)

        return obj.creat_time

    def _get_dav_getcontenttype(self,uri):
        """ find out yourself! """
        return 'application/octet-stream'

    def put(self, uri, data, content_type=None):
        """ put the object into the filesystem """
        sess = self.Session()
        path = urlparse.urlparse(uri)[2]
        path_array = path.split('/')
        name = path_array[-1]
        parent_path = string.join(path_array[:-1])
        if parent_path == '':
            parent_path='/'
        parent = sess.query(TreeObject).filter_by(path=parent_path).first()

        if parent == None :
            raise DAV_Error

        obj = TreeObject(name,0,parent.id,0,0,0,0,path)
        
        sess.add(obj)
        sess.commit()

        content = Content(1,base64.b64encode(data), obj.id)
        sess.add(content)
        sess.commit()
        sess.close()

    def mkcol(self,uri):
        """ create a new collection """
        path = urlparse.urlparse(uri)[2]
        print (path)
        sess = self.Session()
        path_array = path.split('/')
        name = path_array[-2]
        parent_path = string.join(path_array[:-2])
        if parent_path == '':
            parent_path='/'
        parent = sess.query(TreeObject).filter_by(path=parent_path).first()

        if parent == None :
            sess.close()
            raise DAV_Error

        obj = TreeObject(name,1,parent.id,0,0,0,0,path)

        sess.add(obj)
        sess.commit()
        

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
        return self.uri2obj(uri) != None

    def is_collection(self,uri):
        """ test if the given uri is a collection """

        return _get_dav_resourcetype(self,uri) == COLLECTION
