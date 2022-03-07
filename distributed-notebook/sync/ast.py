import ast
from copy import copy, deepcopy
from typing import Tuple

from .log import SyncValue
from .object import Pickled

ExplorerActionCopy = 0
ExplorerActionPass = 1

class SyncAST(ast.NodeVisitor):
  """Capture Dependencies"""
  # pylint: disable=too-many-public-methods
  # pylint: disable=invalid-name

  def __init__(self, tree=None):
    self.tree = tree
    self.scope = []
    self.globals = {}
    self.execution_count = 0

  def __getstate__(self):
    return (self.tree, tuple(self.globals.keys()), self.execution_count)

  def __setstate__(self, state):
    self.tree, keys, self.execution_count = state
    self.scope = []
    self.globals = {}
    for key in keys:
      self.globals[key] = None

  def dump(self, meta=None) -> SyncValue:
    return SyncValue(self.execution_count, (self.tree, tuple(self.globals.keys())))

  def diff(self, tree, meta=None) -> Tuple[Pickled, any]:
    self.source = meta
    if self.tree is None:
      self.tree = self.visit(tree)
      return SyncValue(self.execution_count, (self.tree, tuple(self.globals.keys())))
    else:
      incremental = self.visit(tree)
      self.tree.body.extend(incremental.body)
      return SyncValue(self.execution_count, (incremental, tuple(self.globals.keys())))
  
  def update(self, val: SyncValue) -> any:
    if val.tag <= self.execution_count:
      return None

    if self.tree is None:
      self.tree = val.val[0]
    else:
      self.tree.body.extend(val.val[0].body)
    self.globals = {}
    for key in val.val[1]:
      self.globals[key] = None
    self.execution_count = val.tag
    return val.val[0]

  def sync(self, tree, source):
    self.source = source
    if self.tree is None:
      self.tree = self.visit(tree)
      return tree
    else:
      incremental = self.visit(tree)
      # new_body = []
      # for inherited in self.tree.body:
      #   new_body.append(ast.fix_missing_locations(inherited))
      # new_body.extend(tree.body)
      # tree.body[:] = new_body
      self.tree.body.extend(incremental.body)
      return tree

  def visit(self, node, default=None):
    """Visit a node."""
    if default is None:
      default = self.generic_visit
    method = 'visit_' + node.__class__.__name__
    visitor = getattr(self, method, default)
    return visitor(node)

  def generic_visit(self, node, passIfEmpty=False):
    syncNode = node.__class__()
    for field, value in ast.iter_fields(node):
      if isinstance(value, list):
          syncValues = self.filter_list(value, node, passIfEmpty=passIfEmpty)
          setattr(syncNode, field, syncValues)
      elif isinstance(value, ast.AST):
          syncValue = self.visit(value)
          if syncValue != None:
            setattr(syncNode, field, syncValue)
      else:
        setattr(syncNode, field, value)
    ast.copy_location(syncNode, node)
    return syncNode
  
  def visit_global_stmt(self, node):
    """Visit stmt. Remove global expressions"""
    self.generic_visit(node, passIfEmpty=True)
    print("Remove global expression: {}".format(ast.get_source_segment(self.source, node)))
    return None

  def filter_list(self, list, parent, visitor=None, passIfEmpty=False):
    syncValues = []
    for val in list:
      if isinstance(val, ast.AST):
          val = self.visit(val, default=visitor)
          if val is None:
              continue
          elif not isinstance(val, ast.AST):
              syncValues.extend(val)
              continue
      syncValues.append(val)
    if len(syncValues) == 0 and passIfEmpty:
      syncValues.append(ast.copy_location(ast.Pass(), parent))
    return syncValues
    
  def visit_Module(self, node):
    """Visit Module. Initialize scope"""
    print("Entering Module...")
    self.scope.append(node)
    sync_node = ast.Module(
      self.filter_list(node.body, node, visitor=self.visit_global_stmt),
      node.type_ignores
    )
    self.scope.pop()
    return sync_node

  def visit_FunctionDef(self, node):
    """Visit Module. Advance to function scope"""
    # print("Entering Function \"{}\"...".format(node.name))
    # print(ast.dump(node, indent=2))
    self.scope.append(node)
    sync_node = self.generic_visit(node)
    self.scope.pop()
    return sync_node

  def visit_Import(self, node):
    return self.generic_visit(node)
    
  def visit_ImportFrom(self, node):
    return self.generic_visit(node)

  def visit_ClassDef(self, node):
    # self.globals[node.name] = None
    # print("Found class \"{}\" and keep declaration".format(node.name))
    return self.generic_visit(node)

  def visit_Assign(self, node):
    """Visit Assign, Only global remains"""
    if len(self.scope) > 1:
      return node # Skip non-globals
    
    for target in node.targets:
      if isinstance(target, ast.Name):
        self.globals[target.id] = None
        print("Found global \"{}\" and remove assignment".format(target.id))
      # TODO: deal with other assignments

    return None

  def visit_AnnAssign(self, node):
    """Visit AnnAssign, Only global remains"""
    return self.visit_Assign(node)

  def visit_Global(self, node):
    """Visit Global, Noted"""
    for name in node.names:
      self.globals[name] = None
      print("Found global \"{}\" and keep declaration".format(name))
    return node

  def visit_Delete(self, node):
    """Visit Assign, Only global remains"""
    if len(self.scope) > 1:
      return node # Skip non-globals
    
    for target in node.targets:
      if isinstance(target, ast.Name):
        del self.globals[target.id]
        print("Remove global \"{}\"".format(target.id))
      # TODO: deal with other assignments

    return None
  