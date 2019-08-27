from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import BLOB
from rdr_service.model.field_types import BlobUTF8

# In MySQL, make BLOB fields be LONGBLOB (which supports large blobs). This
# is required for (at least) Questionnaires with, for example, 1000+ questions.
@compiles(BLOB, "mysql")
@compiles(BlobUTF8, "mysql")
def compile_blob_in_mysql_to_longblob(type_, compiler, **kw):
    # pylint: disable=unused-argument
    return "LONGBLOB"
