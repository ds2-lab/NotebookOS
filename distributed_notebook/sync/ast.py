import ast
import logging
from copy import copy, deepcopy
from typing import Tuple, Any, Optional

from .log import SyncValue
from .errors import SyncError

ExplorerActionCopy = 0
ExplorerActionPass = 1

# SyncAST tries to capture runtime states of a Jupyter notebook by visiting AST to find all global variables.
# Corner cases: [A] not implemented yet [B] hard to implement [C] will not implement
# * General file descriptor [A]
# * General network connection [A]
# * Configurations to packages or package managed global variables [C]
class SyncAST(ast.NodeVisitor):
  """Capture Dependencies"""
  # pylint: disable=too-many-public-methods
  # pylint: disable=invalid-name
  _source: str

  def __init__(self, tree=None):
    self._tree = tree
    self._scope = []
    self._globals = {}
    self._executions = 0
    self._log = logging.getLogger(__class__.__name__)
    self._log.setLevel(logging.WARN)

  def __getstate__(self):
    return (self._tree, tuple(self._globals.keys()), self._executions)

  def __setstate__(self, state):
    self._tree, keys, self._executions = state
    self._scope = []
    self._globals = {}
    for key in keys:
      self._globals[key] = None

  @property
  def execution_count(self) -> int:
    return self._executions

  @property
  def tree(self):
    return self._tree

  @property
  def globals(self):
    return self._globals.keys()

  def dump(self, meta=None) -> SyncValue:
    """Return SyncValue for checkpoint"""
    return SyncValue(self._executions, (self._tree, tuple(self._globals.keys())))

  def diff(self, raw, meta=None) -> Optional[SyncValue]:
    """Update AST with the AST of incremental execution and return SyncValue 
       for synchronization. The execution_count will increase by 1."""
    assert meta != None and isinstance(meta, str)
    # The AST we're comparing against, raw, may be None if the user's code had a syntax error.
    # In this case, we'll just increment our executions and return the existing self._tree field.
    if raw != None:
      try:
        self._source = meta
        ret = None
        if self._tree is None:
          self._tree = self.visit(raw)
          ret = self._tree
        else:
          incremental = self.visit(raw)
          self._tree.body.extend(incremental.body)
          ret = incremental
      except Exception as e:
        self._log.error("Failed to diff AST: {}".format(e))
        raise e
    else:
      # Reuse the existing _tree field.
      ret = self._tree

    self._executions = self._executions+1
    return SyncValue(self._executions, (ret, tuple(self._globals.keys())))
  
  def update(self, val: SyncValue) -> Any:
    """Apply the AST of incremental execution to the full AST. 
       Raising exception if the exection count is not the immediate
       next execution."""
    if self._tree is None:
      # Restore
      self._tree = val.val[0]
    elif val.tag <= self._executions:
      # Update but execution count dismatch.
      raise SyncError("Failed to update AST, expects large than {}, but got {}".format(self._executions, val.tag))
    else:
      # Update
      self._tree.body.extend(val.val[0].body)
      
    self._globals = {}
    for key in val.val[1]:
      self._globals[key] = None
    self._executions = val.tag
    return val.val[0]

  def sync(self, tree, source):
    """Legacy method that applies incremental AST to the full AST."""
    self._source = source
    if self._tree is None:
      self._tree = self.visit(tree)
      return tree
    else:
      incremental = self.visit(tree)
      # new_body = []
      # for inherited in self._tree.body:
      #   new_body.append(ast.fix_missing_locations(inherited))
      # new_body.extend(tree.body)
      # tree.body[:] = new_body
      self._tree.body.extend(incremental.body)
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
    # self._log.debug("Remove global expression: {}".format(ast.get_source_segment(self._source, node)))
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
    self._log.debug("Entering Module...")
    self._scope.append(node)
    sync_node = ast.Module(
      self.filter_list(node.body, node, visitor=self.visit_global_stmt),
      node.type_ignores
    )
    self._scope.pop()
    return sync_node

  def visit_FunctionDef(self, node):
    """Visit Function. Advance to function scope to skip local variables"""
    # self._log.debug("Entering Function \"{}\"...".format(node.name))
    # self._log.debug(ast.dump(node, indent=2))
    self._scope.append(node)
    sync_node = self.generic_visit(node)
    self._scope.pop()
    return sync_node

  def visit_Import(self, node):
    return self.generic_visit(node)
    
  def visit_ImportFrom(self, node):
    return self.generic_visit(node)

  def visit_ClassDef(self, node):
    """Visit Class. Advance to class scope to skip class variables"""
    # self._globals[node.name] = None
    # self._log.debug("Found class \"{}\" and keep declaration".format(node.name))
    self._scope.append(node)
    sync_node = self.generic_visit(node)
    self._scope.pop()
    return sync_node

  def visit_Assign(self, node):
    """Visit Assign, Only global remains"""
    if len(self._scope) > 1:
      return node # Skip non-globals
    
    for target in node.targets:
      if isinstance(target, ast.Name):
        self._globals[target.id] = None
        self._log.debug("Found global \"{}\" and remove assignment".format(target.id))
      # TODO: deal with other assignments

    return None

  def visit_AnnAssign(self, node):
    """Visit AnnAssign, Only global remains"""
    return self.visit_Assign(ast.Assign(node))

  def visit_Global(self, node):
    """Visit Global, Noted"""
    for name in node.names:
      self._globals[name] = None
      self._log.debug("Found global \"{}\" and keep declaration".format(name))
    return node

  def visit_Delete(self, node):
    """Visit Assign, Only global remains"""
    if len(self._scope) > 1:
      return node # Skip non-globals
    
    for target in node.targets:
      if isinstance(target, ast.Name):
        del self._globals[target.id]
        self._log.debug("Remove global \"{}\"".format(target.id))
      # TODO: deal with other assignments

    return None
  