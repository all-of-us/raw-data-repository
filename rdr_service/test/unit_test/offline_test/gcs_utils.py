from rdr_service.api_util import open_cloud_file


def assertCsvContents(test, bucket_name, file_name, contents):
    reader = open_cloud_file("%s%s" % (bucket_name, file_name))
    rows = sorted(reader)

    test.assertEqual(sorted(contents), rows)
