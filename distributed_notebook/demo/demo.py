import argparse
import os
import sys
import ast
import logging
import asyncio

from ..sync import Synchronizer, FileLog, RaftLog, CHECKPOINT_AUTO, CHECKPOINT_ON_CHANGE

logging.basicConfig(level=logging.INFO)

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

async def demo():
  parser = argparse.ArgumentParser(description=__doc__, formatter_class=SmartFormatter)
  parser.add_argument("-v", "--version", action="version",
                      version="demo {}".format("0.1"))
  parser.add_argument("--resume", action=argparse.BooleanOptionalAction, help="Resume last execution.")
  parser.add_argument("--replica", action=argparse.BooleanOptionalAction, help="Add 1 replica.")
  parser.add_argument("--replicas", action='store', type=int, help="The number of replicas to add.")
  parser.add_argument("scripts", nargs=argparse.REMAINDER, action=ScriptAction,
                help="Python script to be executed.")
  if len(sys.argv) == 1:
      sys.argv.append("-h")

  try:
    args, _ = parser.parse_known_args()
    if os.path.exists(store) and not args.resume:
      os.system("rm -rf " + store)

    execution_count = 0
    synclog = None
    synchronizer = None
    replicas = ["http://127.0.0.1:19800"]
    if args.replica:
      replicas.append("http://127.0.0.1:19801")
    for i in range(args.replicas):
      replicas.append("http://127.0.0.1:{}".format(19800+i+1))

    for path in args.scripts:
      if synclog is None or synchronizer is None:
        synclog = RaftLog("", 1, replicas)
        # synclog = FileLog(store)
        synchronizer = Synchronizer(synclog, opts=CHECKPOINT_AUTO)
        synchronizer.start()

      execution_count = await synchronizer.ready(execution_count)
      if execution_count == 0:
        raise Exception("Failed to lead the exection.")

      # Load script file
      print("executing {}({})".format(path, execution_count))
      sourceFile = open(path, "r")
      source = sourceFile.read()
      sourceFile.close()

      # Parse and compile the script
      tree = ast.parse(source, path, "exec")
      # print("compiling tree...\n{}".format(ast.dump(tree, indent=2)))
      compiled = compile(tree, path, "exec")
    
      # Execute
      synchronizer.global_ns.setdefault("__dir__", os.path.dirname(path))
      synchronizer.global_ns.setdefault("__file__", path)
      old_main_modules = sys.modules["__main__"]
      sys.modules["__main__"] = synchronizer.module
      sys.path[0] = os.path.dirname(path)
      exec(compiled, synchronizer.global_ns, synchronizer.global_ns) # use namespace for both global and local namespace
      sys.path[0] = base
      sys.modules["__main__"] = old_main_modules

      # Increase execution_count
      execution_count = execution_count + 1

      await synchronizer.sync(tree, source)

    await asyncio.Future()

    synchronizer.close()
    # await future
    synclog = None
    synchronizer = None

  except Exception as exc:
    print(exc)

# def sync(synchronizer=None, module=None, path=None):
#   if synchronizer is None:
#     synchronizer = SyncAST()
#     module = types.ModuleType("__main__", doc="Automatically created module for python environment")
#     # load
#     if not os.path.exists(store+"tree.dat"):
#       return synchronizer, module

#     with open(store + "tree.dat", "rb") as file:
#       synchronizer = pickle.load(file)
#       print("loaded history:\n{}".format(ast.dump(synchronizer.tree)))

#     # Redeclare modules, classes, and functions.
#     compiled = compile(synchronizer.tree, path, "exec")
#     module.__dict__.setdefault("__dir__", os.path.dirname(path))
#     module.__dict__.setdefault("__file__", path)
#     sys.path[0] = os.path.dirname(path)
#     exec(compiled, module.__dict__, module.__dict__)
#     sys.path[0] = base
    
#     with open(store + "data.dat", "rb") as file:
#       old_main_modules = sys.modules["__main__"]
#       sys.modules["__main__"] = module
#       data = pickle.load(file)
#       sys.modules["__main__"] = old_main_modules
#       for key in data.keys():
#         module.__dict__[key] = data[key]
#       return synchronizer, module
#   else:
#     ns = {}
#     for key in synchronizer.globals.keys():
#       ns[key] = module.__dict__[key]

#     if not os.path.exists(store):
#       os.mkdir(store, 0o755)

#     with open(store + "tree.dat", "wb") as file:
#       pickle.dump(synchronizer, file)
#     with open(store + "data.dat", "wb") as file:
#       old_main_modules = sys.modules["__main__"]
#       sys.modules["__main__"] = module
#       pickle.dump(ns, file)
#       sys.modules["__main__"] = old_main_modules
#     return synchronizer, module