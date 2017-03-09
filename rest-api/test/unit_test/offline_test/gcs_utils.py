import csv

from cloudstorage import cloudstorage_api

def assertCsvContents(test, bucket_name, file_name, contents):
  with cloudstorage_api.open('/%s/%s' % (bucket_name, file_name), mode='r') as output:
    reader = csv.reader(output)
    rows = sorted(reader)
  test.assertEquals(sorted(contents), rows)


