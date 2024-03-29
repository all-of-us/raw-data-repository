"""Checks the licenses of all installed packages.

Goes through the list of installed packages, ensuring that each has a license
that we have in allowed list.
"""

import argparse
import email.parser
import os

import pkg_resources


class InvalidLicenseException(BaseException):
    pass


def check_licenses(allowed_list, root, exceptions):
    """Enumerates the installed packages checking licenses against the allowed list.

  Args:
    allowed_list: A list of strings, each the name of a supported license.
    root: Ignore packages outside this directory path.
    exceptions: List of package names to ignore.

  Raises:
    InvalidLicenseException: If a license for an installed package is not in the allowed list.
  """
    installed = pkg_resources.WorkingSet()

    print(("--- Checking packages installed under {} ---".format(root)))

    for pkg in installed:
        if not os.path.commonprefix([pkg.location, root]) == root:
            # This package is not under root.
            print((" {} : {} not under root.".format(pkg.project_name, pkg.location)))
            continue

        if pkg.project_name in exceptions:
            print(("{} is a known exception".format(pkg.project_name)))
            continue

        pkg_info = _load_metadata(pkg)
        classifiers = pkg_info.get_all("Classifier") or []
        license_checked = False
        for classifier in classifiers:
            # Classifiers look like:  "License :: OSI Approved :: MIT License"
            segments = [s.strip() for s in classifier.split("::")]
            if segments[0] == "License":
                segments = segments[1:]
            else:
                continue  # We only care about License entries.
            if segments[0] == "OSI Approved":
                segments = segments[1:]
            if segments:
                for segment in segments:
                    _verify_license(pkg, segment, allowed_list)
                    license_checked = True

        if license_checked:
            # If a license in the classifiers is verified, don't check the old-style
            # (less standard) license tag.
            continue

        _verify_license(pkg, pkg_info["License"], allowed_list)


def _verify_license(pkg, lic, allowed_list):
    pkgname = pkg.project_name
    if lic not in allowed_list:
        raise InvalidLicenseException(
            '{} has unknown license "{}"\n{} is installed in: {}\nKnown licenses are {}.'.format(
                pkgname, lic, pkgname, pkg.location, allowed_list
            )
        )
    print(("{} : {} OK!".format(pkgname, lic)))


def _load_metadata(pkg):
    eparser = email.parser.HeaderParser()
    if pkg.has_metadata("PKG-INFO"):
        return eparser.parsestr(pkg.get_metadata("PKG-INFO"))
    else:
        return eparser.parsestr(pkg.get_metadata("METADATA"))


def _load_and_strip(filename):
    with open(filename) as f:
        stripped = [l.strip("\n \t") for l in f.readlines()]
        comments_removed = [l for l in stripped if l and l[0] != "#"]
        return comments_removed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--licenses_file", help="File containing acceptable licenses, one per line.")
    parser.add_argument("--root", help="Only check packages under this dir.", default=os.path.sep)
    parser.add_argument("--exceptions_file", help="Comma seperated packages lacking metadata.")
    args = parser.parse_args()

    ignore_packages = []
    if args.exceptions_file:
        ignore_packages = _load_and_strip(args.exceptions_file)
    check_licenses(_load_and_strip(args.licenses_file), args.root, ignore_packages)
