# To change this template, choose Tools | Templates
# and open the template in the editor.

class TreeObject:
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


