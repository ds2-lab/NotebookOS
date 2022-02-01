import argparse
import os
import pickle
import sys
import ast
from ..sync.synchronizer import Synchronizer

base = os.path.dirname(os.path.realpath(__file__))
store = base + "/store/"

class SmartFormatter(argparse.HelpFormatter):
  """Add option to split lines in help messages"""

  def _split_lines(self, text, width):
    # this is the RawTextHelpFormatter._split_lines
    if text.startswith('R|'):
        return text[2:].splitlines()
    return argparse.HelpFormatter._split_lines(self, text, width)

class ScriptAction(argparse.Action):                                               # pylint: disable=too-few-public-methods
    """Action to create script attribute"""
    def __call__(self, parser, namespace, values, option_string=None):
        if not values:
            raise argparse.ArgumentError(
                self, "can't be empty")

        scripts = []
        for value in values:
          script = os.path.realpath(value)

          if not os.path.exists(script):
              raise argparse.ArgumentError(
                  self, "can't open file '{}': "
                  "No such file or directory".format(value))

          scripts.append(script)

        setattr(namespace, self.dest, scripts)
        setattr(namespace, "argv", values) 

def demo():
  parser = argparse.ArgumentParser(description=__doc__, formatter_class=SmartFormatter)
  parser.add_argument("-v", "--version", action="version",
                      version="demo {}".format("0.1"))
  parser.add_argument("scripts", nargs=argparse.REMAINDER, action=ScriptAction,
                help="Python script to be executed")
  if len(sys.argv) == 1:
      sys.argv.append("-h")
  if os.path.exists(store):
      os.system("rm -rf " + store)

  try:
    args, _ = parser.parse_known_args()
    namespace = {}
    for path in args.scripts:
      synchronizer, namespace = sync()
      if synchronizer.tree != None:
        print("loaded history:\n{}".format(ast.dump(synchronizer.tree)))

      print("executing {}".format(path))
      sourceFile = open(path, "r")
      source = sourceFile.read()
      sourceFile.close()
      tree = ast.parse(source, path, "exec")
      # print("exec tree:\n{}".format(ast.dump(tree, indent=2)))
      # print("exec tree:\n{}".format(ast.dump(tree)))
      tree = synchronizer.sync(tree, source)
      # print("merged tree:\n{}".format(ast.dump(tree)))
      compiled = compile(tree, path, "exec")
      namespace["__name__"] = "__main__"
      namespace["__dir__"] = os.path.dirname(path)
      namespace["__file__"] = path

      # Execute
      sys.path[0] = namespace["__dir__"]
      exec(compiled, namespace)
      sys.path[0] = base

      sync(synchronizer, namespace)
      # print("synced tree:\n{}".format(ast.dump(synchronizer.tree)))
      synchronizer = None
      namespace = None

  except RuntimeError as exc:
    print(exc)

def sync(synchronizer=None, namespace=None):
  if synchronizer is None:
    synchronizer = Synchronizer()
    # load
    if not os.path.exists(store+"tree.dat"):
      return synchronizer, {}

    with open(store + "tree.dat", "rb") as file:
      dumped = pickle.load(file)
      synchronizer.restore(dumped)
    
    with open(store + "data.dat", "rb") as file:
      data = pickle.load(file)
      return synchronizer, data
  else:
    dumping = synchronizer.dump()
    ns = {}
    for key in synchronizer.globals.keys():
      ns[key] = namespace[key]

    if not os.path.exists(store):
      os.mkdir(store, 0o755)

    with open(store + "tree.dat", "wb") as file:
      pickle.dump(dumping, file)
    with open(store + "data.dat", "wb") as file:
      pickle.dump(ns, file)
    return synchronizer, namespace
