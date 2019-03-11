#
# Helpers for managing replaceable objects in the database such as Views,
# Stored Procedures, Functions and Triggers.
#
# See: https://alembic.sqlalchemy.org/en/latest/cookbook.html#replaceable-objects
#
from alembic.operations import Operations, MigrateOperation


class ReplaceableObject(object):
  """
  Basic Replaceable Object handler
  """

  def __init__(self, name, sqltext):
    self.name = name
    self.sqltext = sqltext


class ReversibleOp(MigrateOperation):
  def __init__(self, target):
    self.target = target

  @classmethod
  def invoke_for_target(cls, operations, target):
    op = cls(target)
    return operations.invoke(op)

  def reverse(self):
    raise NotImplementedError()

  @classmethod
  def _get_object_from_version(cls, operations, ident):
    version, objname = ident.split(".")

    module = operations.get_context().script.get_revision(version).module
    obj = getattr(module, objname)
    return obj

  @classmethod
  def replace(cls, operations, target, replaces=None, replace_with=None):

    if replaces:
      old_obj = cls._get_object_from_version(operations, replaces)
      drop_old = cls(old_obj).reverse()
      create_new = cls(target)
    elif replace_with:
      old_obj = cls._get_object_from_version(operations, replace_with)
      drop_old = cls(target).reverse()
      create_new = cls(old_obj)
    else:
      raise TypeError("replaces or replace_with is required")

    operations.invoke(drop_old)
    operations.invoke(create_new)


@Operations.register_operation("create_view", "invoke_for_target")
@Operations.register_operation("replace_view", "replace")
class CreateViewOp(ReversibleOp):
  def reverse(self):
    return DropViewOp(self.target)


@Operations.register_operation("drop_view", "invoke_for_target")
class DropViewOp(ReversibleOp):
  def reverse(self):
    return CreateViewOp(self.view)


@Operations.register_operation("create_sp", "invoke_for_target")
@Operations.register_operation("replace_sp", "replace")
class CreateSPOp(ReversibleOp):
  def reverse(self):
    return DropSPOp(self.target)


@Operations.register_operation("drop_sp", "invoke_for_target")
class DropSPOp(ReversibleOp):
  def reverse(self):
    return CreateSPOp(self.target)

@Operations.register_operation("create_fn", "invoke_for_target")
@Operations.register_operation("replace_fn", "replace")
class CreateFNOp(ReversibleOp):
  def reverse(self):
    return DropFNOp(self.target)

@Operations.register_operation("drop_fn", "invoke_for_target")
class DropFNOp(ReversibleOp):
  def reverse(self):
    return CreateFNOp(self.target)


@Operations.implementation_for(CreateViewOp)
def create_view(operations, operation):
  operations.execute("CREATE VIEW `{0}` AS {1}".format(
        operation.target.name, operation.target.sqltext))

@Operations.implementation_for(DropViewOp)
def drop_view(operations, operation):
  operations.execute("DROP VIEW `{0}`".format(operation.target.name))


@Operations.implementation_for(CreateSPOp)
def create_sp(operations, operation):
  operations.execute("CREATE PROCEDURE `{0}` {1}".format(
        operation.target.name, operation.target.sqltext))

@Operations.implementation_for(DropSPOp)
def drop_sp(operations, operation):
  operations.execute("DROP PROCEDURE `{0}`".format(operation.target.name))


@Operations.implementation_for(CreateFNOp)
def create_fn(operations, operation):
  operations.execute("CREATE FUNCTION `{0}` {1}".format(
        operation.target.name, operation.target.sqltext))

@Operations.implementation_for(DropFNOp)
def drop_fn(operations, operation):
  operations.execute("DROP FUNCTION `{0}`".format(operation.target.name))
