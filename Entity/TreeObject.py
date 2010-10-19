# To change this template, choose Tools | Templates
# and open the template in the editor.
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
class TreeObject(Base):
    __tablename__= 'TreeObjects'
    id      = Column(Integer, primary_key=True)
    name    = Column(String)
    type    = Column(Integer)
    parent  = Column(Integer)
    owner   = Column(Integer)
    group   = Column(Integer)
    size    = Column(Integer)
    content = Column(Integer)

    def __init__(self, name, type, parent, owner, group, size, content ):
         self.name      = name
         self.type      = type
         self.parent    = parent
         self.owner     = owner
         self.group     = group
         self.size      = size
         self.content   = content

    def __repr__(self):
             return "<TreeObject('%s','%s','%s','%s','%s','%s','%s')>" % (
         self.name, self.type, self.parent, self.owner, self.group, self.size, self.content)


