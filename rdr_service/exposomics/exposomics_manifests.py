from abc import ABC, abstractmethod


class ExposomicsManifestWorkflow(ABC):

    # def __init__(self):
    #     self.destination_path = None
    #     self.source_query = None
    #     self.dao = None

    @abstractmethod
    def get_source_data(self):
        ...


class ExposomicsM0Workflow(ExposomicsManifestWorkflow):

    def __init__(self):
        self.dao = ''
        self.destination_path = ''
        self.source_query = ''

    def get_source_data(self):
        ...


class ExposomicsM1Workflow(ExposomicsManifestWorkflow):

    def get_source_data(self):
        ...
