import cStringIO
import codecs
import csv
# Writes CSV files with Unicode encoding. (Standard CSV libraries don't handle Unicode.)
# Adapted from https://docs.python.org/2/library/csv.html
# Also reads CSV files with Unicode encoding. Adapted from:
# https://gist.github.com/eightysteele/1174811

#
DELIMITER = ','

class UnicodeWriter:
  """
  A CSV writer which will write rows to CSV file "f",
  which is encoded in the given encoding.
  """

  def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
    # Redirect output to a queue
    self.queue = cStringIO.StringIO()
    self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
    self.stream = f
    self.encoder = codecs.getincrementalencoder(encoding)()

  def writerow(self, row):
    self.writer.writerow([s.encode("utf-8") if isinstance(s, unicode) else s for s in row])
    # Fetch UTF-8 output from the queue ...
    data = self.queue.getvalue()
    data = data.decode("utf-8")
    # ... and reencode it into the target encoding
    data = self.encoder.encode(data)
    # write to the target stream
    self.stream.write(data)
    # empty queue
    self.queue.truncate(0)

  def writerows(self, rows):
    for row in rows:
      self.writerow(row)

class UTF8Recoder:
  """
  Iterator that reads an encoded stream and reencodes the input to UTF-8
  """
  def __init__(self, f, encoding):
    self.reader = codecs.getreader(encoding)(f)

  def __iter__(self):
    return self

  def next(self):
    return self.reader.next().encode("utf-8")

class UnicodeDictReader:
  """
  A CSV reader which will iterate over lines in the CSV file "f",
  which is encoded in the given encoding.
  """

  def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
    f = UTF8Recoder(f, encoding)
    self.reader = csv.reader(f, dialect=dialect, **kwds)
    self.fieldnames = self.reader.next()


  def next(self):
    row = self.reader.next()
    vals = [unicode(s, "utf-8") for s in row]
    return dict((self.fieldnames[x], vals[x]) for x in range(len(self.fieldnames)))

  def __iter__(self):
    return self
