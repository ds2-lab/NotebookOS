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

        setattr(namespace, self.dest, values[0])
        setattr(namespace, "argv", values) 

async def replica():
  parser = argparse.ArgumentParser(description=__doc__, formatter_class=SmartFormatter)
  parser.add_argument("--replicas", action='store', type=int, help="The number of replicas to add.", default=1)
  parser.add_argument("-v", "--version", action="version", version="replica {}".format("0.1"))
  parser.add_argument("replica", nargs=argparse.REMAINDER, action=ScriptAction, type=int, help="Replica ID.")

  if len(sys.argv) == 1:
      sys.argv.append("-h")

  try:
    args, _ = parser.parse_known_args()

    replicas = ["http://127.0.0.1:19800"]
    for i in range(args.replicas):
      replicas.append("http://127.0.0.1:{}".format(19800+i+1))

    synclog = RaftLog("", args.replica+1, replicas)
    # synclog = FileLog(store)
    synchronizer = Synchronizer(synclog, opts=CHECKPOINT_AUTO)
    synchronizer.start()

    await asyncio.Future()

    synchronizer.close()

  except Exception as exc:
    print(exc)