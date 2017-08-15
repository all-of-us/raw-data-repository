class ResizingList(list):
  """A list of values that automatically grows when items are set, and returns None if an index
  greater than the list length is provided."""
  def __setitem__(self, index, value):
    if index >= len(self):
      self.extend([None] * (index + 1 - len(self)))
    list.__setitem__(self, index, value)
  
  def __getitem__(self, index):
    if index >= len(self):
      return None
    return list.__getitem__(self, index)