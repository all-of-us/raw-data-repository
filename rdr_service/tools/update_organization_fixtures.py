#!/usr/bin/env python
# coding=utf-8
"""
Updates the test fixture data files in `test/test-data/fixtures` from the current files in
`data/*.csv`.

Creates sanitized copies of them to live in the repository.

"""
import argparse
import csv
import itertools
import os
import shutil


parser = argparse.ArgumentParser()
parser.add_argument('--source', default='data')
parser.add_argument('--destination', default='test/test-data/fixtures')


def main(source, destination):
  """
  Create sanitized versions of the ORGANIZATION data files from `source` in `destination`.
  """
  copy_file(
    os.path.join(source, 'awardees.csv'),
    os.path.join(destination, 'awardees.csv')
  )
  copy_file(
    os.path.join(source, 'organizations.csv'),
    os.path.join(destination, 'organizations.csv')
  )
  copy_sanitized_csv(
    os.path.join(source, 'sites.csv'),
    os.path.join(destination, 'sites.csv'),
    itertools.repeat({
      'Notes Spanish': 'Notas',
      'Notes': 'Notes',
      'Scheduling Instructions': 'Scheduling Instructions',
      'Scheduling Instructions Spanish': 'Instrucciones de programación',
      'Directions': 'Directions',
      'Physical Location Name': 'Location',
      'Address 1': 'Address 1',
      'Address 2': 'Address 2',
      'City': 'Washington',
      'State': 'DC',
      'Zip': '00000',
      'Phone': '555-555-5555',
      'Admin Email Addresses': 'fake@test.faketld',
    })
  )


def copy_file(source, destination):
  """
  Directly copy a file, no sanitization steps necessary.
  """
  with open(source, 'r') as source_file:
    with open(destination, 'w') as destination_file:
      shutil.copyfileobj(source_file, destination_file)


def copy_sanitized_csv(source, destination, overrides_iterable):
  """
  Copy a CSV file overriding the specified values.
  Assumes header rows are present in source file.
  """
  with open(source, 'r') as source_file:
    reader = csv.DictReader(source_file)
    with open(destination, 'w') as destination_file:
      writer = csv.DictWriter(destination_file, reader.fieldnames)
      writer.writeheader()
      for row, overrides in zip(reader, overrides_iterable):
        writer.writerow(dict(row, **overrides))


if __name__ == '__main__':
  args = parser.parse_args()
  main(**vars(args))
