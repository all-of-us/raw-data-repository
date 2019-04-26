#! /usr/bin/env python
#
# RDR cli tool launcher
#

# pylint: disable=superfluous-parens
import copy
import glob
import importlib
import os
import sys

lib_paths = ['../service_libs', 'service_libs']
import_path = 'service_libs'


def run():
  args = copy.deepcopy(sys.argv)

  show_usage = False
  group = 'no-group'

  # If help is select lets build a list of commands to show
  if '--help' == sys.argv[1] or '-h' == sys.argv[1]:
    show_usage = True

  # If not showing help, get the group name and then we'll call it.
  if not show_usage:
    group = args.pop(1)
    sys.argv = args

  lp = None
  for lib_path in lib_paths:
    if os.path.exists(os.path.join(os.curdir, lib_path)):
      lp = os.path.join(os.curdir, lib_path)

  if not lp:
    print('ERROR: service libs path not found, aborting.')
    exit(1)

  group_names = list()

  libs = glob.glob(os.path.join(lp, '*.py'))
  for lib in libs:

    mod_name = os.path.basename(lib).split('.')[0]
    mod = importlib.import_module('{0}.{1}'.format(import_path, mod_name))

    if hasattr(mod, 'group'):

      if show_usage:
        if mod.group != 'template':
          group_names.append('  {0} : {1}'.format(mod.group.ljust(14), mod.group_desc))
      else:
        if mod.group == group:
          mod.run()
          break

  if show_usage:
    print('\nusage: rdr.py command [-h|--help] [args]\n\navailable commands:')

    group_names.sort()
    for gn in group_names:
      print(gn)

    print('')

# --- Main Program Call ---
if __name__ == '__main__':
  sys.exit(run())
