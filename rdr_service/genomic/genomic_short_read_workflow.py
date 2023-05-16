import csv
import logging

from rdr_service import clock
from rdr_service.config import GENOMIC_INVESTIGATION_GENOME_TYPES, GENOME_TYPE_ARRAY, GENOME_TYPE_WGS
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicSetDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.genomic.genomic_state_handler import GenomicStateHandler
from rdr_service.genomic_enums import GenomicWorkflowState, GenomicSubProcessResult, GenomicIncidentCode, GenomicJob, \
    GenomicSetMemberStatus
from rdr_service.model.config_utils import get_biobank_id_prefix
from rdr_service.model.genomics import GenomicSetMember, GenomicSet, GenomicSampleContamination


class BaseShortReadWorkflow:

    def __init__(self, genomic_job_controller, file_obj):
        self.genomic_job_controller = genomic_job_controller
        self.file_obj = file_obj


class GenomicAW1Workflow(BaseShortReadWorkflow):

    @classmethod
    def get_aw1_manifest_column_mappings(cls):
        return {
            'packageId': 'packageid',
            'sampleId': 'sampleid',
            'gcManifestBoxStorageUnitId': 'boxstorageunitid',
            'gcManifestBoxPlateId': 'boxid/plateid',
            'gcManifestWellPosition': 'wellposition',
            'gcManifestParentSampleId': 'parentsampleid',
            'collectionTubeId': 'collectiontubeid',
            'gcManifestMatrixId': 'matrixid',
            'gcManifestTreatments': 'treatments',
            'gcManifestQuantity_ul': 'quantity(ul)',
            'gcManifestTotalConcentration_ng_per_ul': 'totalconcentration(ng/ul)',
            'gcManifestTotalDNA_ng': 'totaldna(ng)',
            'gcManifestVisitDescription': 'visitdescription',
            'gcManifestSampleSource': 'samplesource',
            'gcManifestStudy': 'study',
            'gcManifestTrackingNumber': 'trackingnumber',
            'gcManifestContact': 'contact',
            'gcManifestEmail': 'email',
            'gcManifestStudyPI': 'studypi',
            'gcManifestTestName': 'genometype',
            'gcManifestFailureMode': 'failuremode',
            'gcManifestFailureDescription': 'failuremodedesc',
        }

    def _get_site_from_aw1(self):
        """
        Returns the Genomic Center's site ID from the AW1 filename
        :return: GC site ID string
        """
        return self.file_obj.fileName.split('/')[-1].split("_")[0].lower()

    def ingest_single_aw1_row_for_member(self, member):
        member_dao = GenomicSetMemberDao()

        from rdr_service.genomic.genomic_job_components import GenomicFileIngester

        # Open file and pull row based on member.biobankId
        with self.genomic_job_controller.storage_provider.open(self.file_obj.file_path[1:], 'r') as aw1_file:
            reader = csv.DictReader(aw1_file, delimiter=',')
            row = [r for r in reader if r['BIOBANK_ID'][1:] == str(member.biobankId)][0]

            # Alter field names to remove spaces and change to lower case
            row = GenomicFileIngester.clean_row_keys(row)

        ingested_before = member.reconcileGCManifestJobRunId is not None

        # Write AW1 data to genomic_set_member table
        gc_manifest_column_mappings = self.get_aw1_manifest_column_mappings()

        # Set attributes from file
        for key in gc_manifest_column_mappings.keys():
            try:
                member.__setattr__(key, row[gc_manifest_column_mappings[key]])
            except KeyError:
                member.__setattr__(key, None)

        # Set other fields not in AW1 file
        member.reconcileGCManifestJobRunId = self.genomic_job_controller.job_run.id
        member.aw1FileProcessedId = self.file_obj.id
        member.gcSite = self._get_site_from_aw1()

        # Only update the member's genomicWorkflowState if it was AW0
        if member.genomicWorkflowState == GenomicWorkflowState.AW0:
            member.genomicWorkflowState = GenomicWorkflowState.AW1
            member.genomicWorkflowStateStr = GenomicWorkflowState.AW1.name
            member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

        # Update member in DB
        member_dao.update(member)
        # Update AW1 manifest record count
        if not ingested_before and not self.genomic_job_controller.bypass_record_count:
            GenomicFileIngester.increment_manifest_file_record_count_from_id(self.file_obj)

        return GenomicSubProcessResult.SUCCESS

    def _process_aw1_attribute_data(self, aw1_data, member):
        """
        Checks a GenomicSetMember object for changes provided by AW1 data
        And mutates the GenomicSetMember object if necessary
        :param aw1_data: dict
        :param member: GenomicSetMember
        :return: (boolean, GenomicSetMember)
        """
        # Check if the member needs updating
        if self._test_aw1_data_for_member_updates(aw1_data, member):
            member = self._set_member_attributes_from_aw1(aw1_data, member)
            member = self._set_rdr_member_attributes_for_aw1(aw1_data, member)
            return True, member
        return False, member

    def _test_aw1_data_for_member_updates(self, aw1_data, member):
        """
        Checks each attribute provided by Biobank
        for changes to GenomicSetMember Object
        :param aw1_data: dict
        :param member: GenomicSetMember
        :return: boolean (true if member requires updating)
        """
        gc_manifest_column_mappings = self.get_aw1_manifest_column_mappings()
        member_needs_updating = False

        # Iterate each value and test whether the strings for each field correspond
        for key in gc_manifest_column_mappings.keys():
            if str(member.__getattribute__(key)) != str(aw1_data.get(gc_manifest_column_mappings[key])):
                member_needs_updating = True

        return member_needs_updating

    def _set_member_attributes_from_aw1(self, aw1_data, member):
        """
        Mutates the GenomicSetMember attributes provided by the Biobank
        :param aw1_data: dict
        :param member: GenomicSetMember
        :return: GenomicSetMember
        """
        gc_manifest_column_mappings = self.get_aw1_manifest_column_mappings()
        for key in gc_manifest_column_mappings.keys():
            member.__setattr__(key, aw1_data.get(gc_manifest_column_mappings[key]))
        return member

    def _set_rdr_member_attributes_for_aw1(self, aw1_data, member):
        """
        Mutates the GenomicSetMember RDR attributes not provided by the Biobank
        :param aw1_data: dict
        :param member: GenomicSetMember
        :return: GenomicSetMember
        """
        # Set job run and file processed IDs
        member.reconcileGCManifestJobRunId = self.genomic_job_controller.job_run.id

        # Don't overwrite aw1_file_processed_id when ingesting an AW1F
        if self.genomic_job_controller.job_id == GenomicJob.AW1_MANIFEST:
            member.aw1FileProcessedId = self.file_obj.id

        # Set the GC site ID (sourced from file-name)
        member.gcSiteId = aw1_data['site_id']

        # Only update the state if it was AW0 or AW1 (if in failure manifest workflow)
        # We do not want to regress a state for reingested data
        states_to_update = [
            GenomicWorkflowState.AW0,
            GenomicWorkflowState.EXTRACT_REQUESTED
        ]

        if self.genomic_job_controller.job_id == GenomicJob.AW1F_MANIFEST:
            states_to_update = [GenomicWorkflowState.AW1]

        if member.genomicWorkflowState in states_to_update:
            _signal = "aw1-reconciled"

            # Set the signal for a failed sample
            if aw1_data['failuremode'] is not None and aw1_data['failuremode'] != '':
                _signal = 'aw1-failed'

            member.genomicWorkflowState = GenomicStateHandler.get_new_state(
                member.genomicWorkflowState,
                signal=_signal)

            member.genomicWorkflowStateStr = member.genomicWorkflowState.name
            member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

        return member

    def create_investigation_member_record_from_aw1(self, aw1_data):
        member_dao = GenomicSetMemberDao()
        set_dao = GenomicSetDao()
        new_set = GenomicSet(
            genomicSetName=f"investigation_{self.genomic_job_controller.job_run.id}",
            genomicSetCriteria="investigation genome type",
            genomicSetVersion=1,
        )

        set_dao.insert(new_set)
        participant_dao = ParticipantDao()

        # Get IDs
        biobank_id = aw1_data['biobankid']

        # Strip biobank prefix if it's there
        if biobank_id[0] in [get_biobank_id_prefix(), 'T']:
            biobank_id = biobank_id[1:]

        participant = participant_dao.get_by_biobank_id(biobank_id)

        # Create new genomic_set_member
        new_member = GenomicSetMember(
            genomicSetId=new_set.id,
            biobankId=biobank_id,
            participantId=participant.participantId,
            reconcileGCManifestJobRunId=self.genomic_job_controller.job_run.id,
            genomeType=aw1_data['genometype'],
            sexAtBirth=aw1_data['sexatbirth'],
            blockResearch=1,
            blockResearchReason="Created from AW1 with investigation genome type.",
            blockResults=1,
            blockResultsReason="Created from AW1 with investigation genome type.",
            genomicWorkflowState=GenomicWorkflowState.AW1,
            genomicWorkflowStateStr=GenomicWorkflowState.AW1.name,
        )

        _, member = self._process_aw1_attribute_data(aw1_data, new_member)
        member_dao.insert(member)

    def create_new_member_from_aw1_control_sample(self, aw1_data: dict) -> GenomicSetMember:
        """
        Creates a new control sample GenomicSetMember in RDR based on AW1 data
        These will look like regular GenomicSetMember samples
        :param aw1_data: dict from aw1 row
        :return:  GenomicSetMember
        """
        member_dao = GenomicSetMemberDao()
        # Writing new genomic_set_member based on AW1 data
        max_set_id = member_dao.get_collection_tube_max_set_id()[0]
        # Insert new member with biobank_id and collection tube ID from AW1
        new_member_obj = GenomicSetMember(
            genomicSetId=max_set_id,
            participantId=0,
            biobankId=aw1_data['biobankid'],
            collectionTubeId=aw1_data['collectiontubeid'],
            validationStatus=GenomicSetMemberStatus.VALID,
            genomeType=aw1_data['genometype'],
            genomicWorkflowState=GenomicWorkflowState.AW1,
            genomicWorkflowStateStr=GenomicWorkflowState.AW1.name
        )

        # Set member attributes from AW1
        new_member_obj = self._set_member_attributes_from_aw1(aw1_data, new_member_obj)
        new_member_obj = self._set_rdr_member_attributes_for_aw1(aw1_data, new_member_obj)

        return member_dao.insert(new_member_obj)

    def ingest_aw1_manifest(self, rows):
        """
        AW1 ingestion method: Updates the GenomicSetMember with AW1 data
        If the row is determined to be a control sample,
        insert a new GenomicSetMember with AW1 data
        :param rows:
        :return: result code
        """
        member_dao = GenomicSetMemberDao()
        sample_dao = BiobankStoredSampleDao()
        _states = [GenomicWorkflowState.AW0, GenomicWorkflowState.EXTRACT_REQUESTED]
        _site = self._get_site_from_aw1()

        from rdr_service.genomic.genomic_job_components import GenomicFileIngester

        for row in rows:
            row_copy = GenomicFileIngester.clean_row_keys(row)

            row_copy['site_id'] = _site
            # Skip rows if biobank_id is an empty string (row is empty well)
            if row_copy['biobankid'] == "":
                continue

            # Check if this sample has a control sample parent tube
            control_sample_parent = member_dao.get_control_sample_parent(
                row_copy['genometype'],
                int(row_copy['parentsampleid'])
            )

            # Create new set member record if the sample
            # has the investigation genome type
            if row_copy['genometype'] in GENOMIC_INVESTIGATION_GENOME_TYPES:
                self.create_investigation_member_record_from_aw1(row_copy)

                # Move to next row in file
                continue

            if control_sample_parent:
                logging.warning(f"Control sample found: {row_copy['parentsampleid']}")

                # Check if the control sample member exists for this GC, BID, collection tube, and sample ID
                # Since the Biobank is reusing the sample and collection tube IDs (which are supposed to be unique)
                cntrl_sample_member = member_dao.get_control_sample_for_gc_and_genome_type(
                    _site,
                    row_copy['genometype'],
                    row_copy['biobankid'],
                    row_copy['collectiontubeid'],
                    row_copy['sampleid']
                )

                if not cntrl_sample_member:
                    # Insert new GenomicSetMember record if none exists
                    # for this control sample, genome type, and gc site
                    self.create_new_member_from_aw1_control_sample(row_copy)
                continue

            # Find the existing GenomicSetMember
            if self.genomic_job_controller.job_id == GenomicJob.AW1F_MANIFEST:
                # Set the member based on collection tube ID will null sample
                member = member_dao.get_member_from_collection_tube(
                    row_copy['collectiontubeid'],
                    row_copy['genometype'],
                    state=GenomicWorkflowState.AW1
                )
            else:
                # Set the member based on collection tube ID will null sample
                member = member_dao.get_member_from_collection_tube_with_null_sample_id(
                    row_copy['collectiontubeid'],
                    row_copy['genometype'])

            # Since member not found, and not a control sample,
            # check if collection tube id was swapped by Biobank
            if not member:
                bid = row_copy['biobankid']

                # Strip biobank prefix if it's there
                if bid[0] in [get_biobank_id_prefix(), 'T']:
                    bid = bid[1:]
                member = member_dao.get_member_from_biobank_id_in_state(
                    bid,
                    row_copy['genometype'],
                    _states
                )
                # If member found, validate new collection tube ID, set collection tube ID
                if member:
                    if GenomicFileIngester.validate_collection_tube_id(row_copy['collectiontubeid'], bid):
                        if member.genomeType in [GENOME_TYPE_ARRAY, GENOME_TYPE_WGS]:
                            if member.collectionTubeId:
                                with member_dao.session() as session:
                                    session.add(GenomicSampleContamination(
                                        sampleId=member.collectionTubeId,
                                        failedInJob=self.genomic_job_controller.job_id
                                    ))

                        member.collectionTubeId = row_copy['collectiontubeid']
                else:
                    # Couldn't find genomic set member based on either biobank ID or collection tube
                    _message = f"{self.genomic_job_controller.job_id.name}: Cannot find genomic set member: " \
                               f"collection_tube_id: {row_copy['collectiontubeid']}, " \
                               f"biobank id: {bid}, " \
                               f"genome type: {row_copy['genometype']}"

                    self.genomic_job_controller.create_incident(
                        source_job_run_id=self.genomic_job_controller.job_run.id,
                        source_file_processed_id=self.file_obj.id,
                        code=GenomicIncidentCode.UNABLE_TO_FIND_MEMBER.name,
                        message=_message,
                        biobank_id=bid,
                        collection_tube_id=row_copy['collectiontubeid'],
                        sample_id=row_copy['sampleid'],
                    )
                    # Skip rest of iteration and continue processing file
                    continue

            # Check for diversion pouch site
            div_pouch_site_id = sample_dao.get_diversion_pouch_site_id(row_copy['collectiontubeid'])
            if div_pouch_site_id:
                member.diversionPouchSiteFlag = 1

            # Process the attribute data
            member_changed, member = self._process_aw1_attribute_data(row_copy, member)
            if member_changed:
                member_dao.update(member)

        return GenomicSubProcessResult.SUCCESS
