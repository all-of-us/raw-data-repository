#! /usr/bin/env python
#
# RDR tool launcher
#

# pylint: disable=superfluous-parens
import copy
import glob
import importlib
import os
import re
import sys


def _grep_prop(filename, prop_name):
    """
    Look for property in file
    :param filename: path to file and file name.
    :param prop_name: property to search for in file.
    :return: property value or None.
    """
    fdata = open(filename, "r").read()
    obj = re.search("^{0} = ['|\"](.+)['|\"]$".format(prop_name), fdata, re.MULTILINE)
    if obj:
        return obj.group(1)
    return None


def _run_tool(lib_paths, import_path):
    """
    Run the tools from the given path.
    """
    # We need to run from the `rdr_service` directory, save the current directory
    cwd = os.path.abspath(os.curdir)
    if not cwd.endswith('rdr_service'):
        tmp_cwd = os.path.join(cwd, 'rdr_service')
        if not os.path.exists(tmp_cwd):
            raise FileNotFoundError('Unable to locate "rdr_service" directory.')
        os.chdir(tmp_cwd)

    args = copy.deepcopy(sys.argv)

    show_usage = False
    command = "no-command"

    # If help is select lets build a list of commands to show
    if len(sys.argv) == 1 or "--help" == sys.argv[1] or "-h" == sys.argv[1]:
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
        print("ERROR: tool library path not found, aborting.")
        os.chdir(cwd)
        exit(1)

    command_names = list()

    libs = glob.glob(os.path.join(lp, "*.py"))
    for lib in libs:
        mod_cmd = _grep_prop(lib, "tool_cmd")
        mod_desc = _grep_prop(lib, "tool_desc")
        if not mod_cmd:
            continue

        if show_usage:
            if mod_cmd != "template":
                command_names.append("  {0} : {1}".format(mod_cmd.ljust(14), mod_desc))
        else:
            if mod_cmd == command:
                mod_name = os.path.basename(lib).split(".")[0]
                mod = importlib.import_module("{0}.{1}".format(import_path, mod_name))
                exit_code = mod.run()
                print("finished.")
                os.chdir(cwd)
                return exit_code

    if show_usage:
        if 'rtool' in sys.argv[0]:
            print("\nusage: rtool command [-h|--help] [args]\n\navailable commands:")
        else:
            print("\nusage: python -m tools command [-h|--help] [args]\n\navailable commands:")

        command_names.sort()
        for gn in command_names:
            print(gn)

        print("")

    os.chdir(cwd)


def run():
    """
    Developer Tools
    """
    lib_paths = ["rdr_service/tools/tool_libs", "tools/tool_libs", "../tool_libs", "tool_libs", "../../tool_libs"]
    import_path = "rdr_service.tools.tool_libs"
    return _run_tool(lib_paths, import_path)


# --- Main Program Call ---
sys.exit(run())
