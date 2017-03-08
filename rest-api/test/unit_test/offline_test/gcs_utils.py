import csv

from cloudstorage import cloudstorage_api

def assertCsvContents(test, bucket_name, file_name, contents):
  with cloudstorage_api.open('/%s/%s' % (bucket_name, file_name), mode='r') as output:
    reader = csv.reader(output)
    for row in contents:
      test.assertEquals(row, reader.next())
    try:
      row = reader.next()
      test.fail("Extra row(s) found: %s" % row)
    except StopIteration:
      pass

