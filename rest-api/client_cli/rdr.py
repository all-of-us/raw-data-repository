#! /usr/bin/env python
#
# RDR cli tool launcher
#

# pylint: disable=superfluous-parens
import copy
import glob
import importlib
import os
import re
import sys

lib_paths = ['../service_libs', 'service_libs']
import_path = 'service_libs'


def _grep_prop(filename, prop_name):
  """
  Look for property in file
  :param filename: path to file and file name.
  :param prop_name: property to search for in file.
  :return: property value or None.
  """
  fdata = open(filename, 'r').read()
  obj = re.search("^{0} = '(.+)'$".format(prop_name), fdata, re.MULTILINE)
  if obj:
    return obj.group(1)
  return None

def run():
  args = copy.deepcopy(sys.argv)

  show_usage = False
  command = 'no-command'

  # If help is select lets build a list of commands to show
  if len(sys.argv) == 1 or '--help' == sys.argv[1] or '-h' == sys.argv[1]:
    show_usage = True

  # If not showing help, get the command name and then we'll call it.
  if not show_usage:
    command = args.pop(1)
    sys.argv = args

  lp = None
  for lib_path in lib_paths:
    if os.path.exists(os.path.join(os.curdir, lib_path)):
      lp = os.path.join(os.curdir, lib_path)

  if not lp:
    print('ERROR: service libs path not found, aborting.')
    exit(1)

  command_names = list()

  libs = glob.glob(os.path.join(lp, '*.py'))
  for lib in libs:
    mod_cmd = _grep_prop(lib, 'mod_cmd')
    mod_desc = _grep_prop(lib, 'mod_desc')
    if not mod_cmd:
      continue

    if show_usage:
      if mod_cmd != 'template':
        command_names.append('  {0} : {1}'.format(mod_cmd.ljust(14), mod_desc))
    else:
      if mod_cmd == command:
        mod_name = os.path.basename(lib).split('.')[0]
        mod = importlib.import_module('{0}.{1}'.format(import_path, mod_name))
        mod.run()
        break

  if show_usage:
    print('\nusage: rdr.py command [-h|--help] [args]\n\navailable commands:')

    command_names.sort()
    for gn in command_names:
      print(gn)

    print('')

# --- Main Program Call ---
if __name__ == '__main__':
  sys.exit(run())
