# To change this template, choose Tools | Templates
# and open the template in the editor.
from Entity.TreeObject import TreeObject
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Content(Base):
    __tablename__= 'Contents'

    id       = Column(Integer, primary_key=True)
    revision = Column(Integer)
    content  = Column(Integer)
    tree_object = Column(Integer)
 

    def __init__(self, revision, content, tree_object):
        self.revision    = revision
        self.content     = content
        self.tree_object = tree_object

    def __repr__(self):
        return "<Content('%s','%s', '%s')>" % (self.tree_object, self.content, self.revision)

