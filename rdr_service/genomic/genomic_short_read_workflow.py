import logging
from abc import ABC, abstractmethod
from typing import List, OrderedDict

from rdr_service import clock, config
from rdr_service.config import GENOMIC_INVESTIGATION_GENOME_TYPES, GENOME_TYPE_ARRAY, GENOME_TYPE_WGS, \
    GENOME_TYPE_WGS_INVESTIGATION
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.genomics_dao import GenomicManifestFeedbackDao, GenomicSetDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.genomic.genomic_mappings import array_file_types_attributes, wgs_file_types_attributes, \
    genome_center_datafile_prefix_map, wgs_metrics_manifest_mapping
from rdr_service.genomic.genomic_state_handler import GenomicStateHandler
from rdr_service.genomic_enums import GenomicWorkflowState, GenomicSubProcessResult, GenomicIncidentCode, GenomicJob, \
    GenomicSetMemberStatus, GenomicContaminationCategory, GenomicQcStatus
from rdr_service.model.config_utils import get_biobank_id_prefix
from rdr_service.model.genomics import GenomicSetMember, GenomicSet, GenomicSampleContamination, \
    GenomicGCValidationMetrics

# pylint: disable=broad-except

class BaseGenomicShortReadWorkflow(ABC):

    def __init__(self, file_ingester):
        self.file_ingester = file_ingester

    @abstractmethod
    def run_ingestion(self, rows: List[OrderedDict]) -> str:
        ...

    @abstractmethod
    def get_gc_data(self):
        ...


class GenomicAW1Workflow(BaseGenomicShortReadWorkflow):

    def get_gc_data(self):
        ...

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
        return self.file_ingester.file_obj.fileName.split('/')[-1].split("_")[0].lower()

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
        member.reconcileGCManifestJobRunId = self.file_ingester.controller.job_run.id

        # Don't overwrite aw1_file_processed_id when ingesting an AW1F
        if self.file_ingester.controller.job_id == GenomicJob.AW1_MANIFEST:
            member.aw1FileProcessedId = self.file_ingester.file_obj.id

        # Set the GC site ID (sourced from file-name)
        member.gcSiteId = aw1_data['site_id']

        # Only update the state if it was AW0 or AW1 (if in failure manifest workflow)
        # We do not want to regress a state for reingested data
        states_to_update = [
            GenomicWorkflowState.AW0,
            GenomicWorkflowState.EXTRACT_REQUESTED
        ]

        if self.file_ingester.controller.job_id == GenomicJob.AW1F_MANIFEST:
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

    def create_new_investigation_genomic_set(self):
        set_dao = GenomicSetDao()
        new_set = GenomicSet(
            genomicSetName=f"investigation_{self.file_ingester.controller.job_run.id}",
            genomicSetCriteria="investigation genome type",
            genomicSetVersion=1,
        )
        with set_dao.session() as session:
            session.add(new_set)
        return new_set

    def create_investigation_member_record_from_aw1(self, aw1_data):
        if not self.file_ingester.investigation_set_id:
            new_set = self.create_new_investigation_genomic_set()
            self.file_ingester.investigation_set_id = new_set.id
        # Get IDs
        biobank_id = aw1_data['biobankid']
        # Strip biobank prefix if it's there
        if biobank_id[0] in [get_biobank_id_prefix(), 'T']:
            biobank_id = biobank_id[1:]

        participant = ParticipantDao().get_by_biobank_id(biobank_id)
        # Create new genomic_set_member
        new_member = GenomicSetMember(
            genomicSetId=self.file_ingester.investigation_set_id,
            biobankId=biobank_id,
            participantId=participant.participantId,
            reconcileGCManifestJobRunId=self.file_ingester.controller.job_run.id,
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
        self.file_ingester.member_dao.insert(member)

    def create_new_member_from_aw1_control_sample(self, aw1_data: dict) -> GenomicSetMember:
        """
        Creates a new control sample GenomicSetMember in RDR based on AW1 data
        These will look like regular GenomicSetMember samples
        :param aw1_data: dict from aw1 row
        :return:  GenomicSetMember
        """
        # Writing new genomic_set_member based on AW1 data
        max_set_id = self.file_ingester.member_dao.get_collection_tube_max_set_id()[0]
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

        return self.file_ingester.member_dao.insert(new_member_obj)

    def run_ingestion(self, rows: List[OrderedDict]) -> str:
        """
        AW1 ingestion method: Updates the GenomicSetMember with AW1 data
        If the row is determined to be a control sample,
        insert a new GenomicSetMember with AW1 data
        :param rows:
        :return: result code
        """
        try:
            sample_dao = BiobankStoredSampleDao()
            workflow_states = [GenomicWorkflowState.AW0, GenomicWorkflowState.EXTRACT_REQUESTED]
            gc_site = self._get_site_from_aw1()

            for row in rows:
                row_copy = self.file_ingester.clean_row_keys(row)

                row_copy['site_id'] = gc_site
                # Skip rows if biobank_id is an empty string (row is empty well)
                if row_copy['biobankid'] == "":
                    continue

                # Check if this sample has a control sample parent tube
                control_sample_parent = self.file_ingester.member_dao.get_control_sample_parent(
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
                    cntrl_sample_member = self.file_ingester.member_dao.get_control_sample_for_gc_and_genome_type(
                        gc_site,
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
                if self.file_ingester.controller.job_id == GenomicJob.AW1F_MANIFEST:
                    # Set the member based on collection tube ID will null sample
                    member = self.file_ingester.member_dao.get_member_from_collection_tube(
                        row_copy['collectiontubeid'],
                        row_copy['genometype'],
                        state=GenomicWorkflowState.AW1
                    )
                else:
                    # Set the member based on collection tube ID will null sample
                    member = self.file_ingester.member_dao.get_member_from_collection_tube_with_null_sample_id(
                        row_copy['collectiontubeid'],
                        row_copy['genometype'])

                # Since member not found, and not a control sample,
                # check if collection tube id was swapped by Biobank
                if not member:
                    bid = row_copy['biobankid']

                    # Strip biobank prefix if it's there
                    if bid[0] in [get_biobank_id_prefix(), 'T']:
                        bid = bid[1:]
                    member = self.file_ingester.member_dao.get_member_from_biobank_id_in_state(
                        bid,
                        row_copy['genometype'],
                        workflow_states
                    )
                    # If member found, validate new collection tube ID, set collection tube ID
                    if member:
                        if self.file_ingester.validate_collection_tube_id(row_copy['collectiontubeid'], bid):
                            if member.genomeType in [GENOME_TYPE_ARRAY, GENOME_TYPE_WGS]:
                                if member.collectionTubeId:
                                    with self.file_ingester.member_dao.session() as session:
                                        session.add(GenomicSampleContamination(
                                            sampleId=member.collectionTubeId,
                                            failedInJob=self.file_ingester.controller.job_id
                                        ))

                            member.collectionTubeId = row_copy['collectiontubeid']
                    else:
                        # Couldn't find genomic set member based on either biobank ID or collection tube
                        _message = f"{self.file_ingester.controller.job_id.name}: Cannot find genomic set member: " \
                                   f"collection_tube_id: {row_copy['collectiontubeid']}, " \
                                   f"biobank id: {bid}, " \
                                   f"genome type: {row_copy['genometype']}"

                        self.file_ingester.controller.create_incident(
                            source_job_run_id=self.file_ingester.controller.job_run.id,
                            source_file_processed_id=self.file_ingester.file_obj.id,
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
                    self.file_ingester.member_dao.update(member)

            return GenomicSubProcessResult.SUCCESS

        except Exception as e:
            logging.warning(f'Error when ingesting AW1 manifest file: {self.file_ingester.file_obj.filePath}: {e}')
            return GenomicSubProcessResult.ERROR


class GenomicAW2Workflow(BaseGenomicShortReadWorkflow):

    def get_gc_data(self):
        gc_site_bucket_map = config.getSettingJson(config.GENOMIC_GC_SITE_BUCKET_MAP, {})
        site_id = self.file_ingester.file_obj.fileName.split('_')[0].lower()
        gc_bucket_name = gc_site_bucket_map.get(site_id)
        config.getSetting(gc_bucket_name, None)
        return site_id, config.getSetting(gc_bucket_name, None)

    def set_metrics_data_file_paths(self, genome_type: str, row: dict) -> dict:
        site_id, gc_bucket = self.get_gc_data()
        if not gc_bucket:
            return row

        data_file_paths_map = {
            config.GENOME_TYPE_ARRAY: {
                'attributes': array_file_types_attributes
            },
            config.GENOME_TYPE_ARRAY_INVESTIGATION: {
                'attributes': array_file_types_attributes
            },
            config.GENOME_TYPE_WGS: {
                'attributes': wgs_file_types_attributes,
                'mappings': wgs_metrics_manifest_mapping
            },
            config.GENOME_TYPE_WGS_INVESTIGATION: {
                'attributes': wgs_file_types_attributes,
                'mappings': wgs_metrics_manifest_mapping
            }
        }[genome_type]

        row_paths = {k: v for k, v in row.items() if 'path' in k and v is not None}
        required_file_paths = list(filter(lambda x: x['required'] is True, data_file_paths_map.get('attributes')))

        # If file paths in manifest then grep from manifest and return (should only apply to WGS @ this time)
        if (len(required_file_paths) == len(row_paths)) and data_file_paths_map.get('mappings'):
            # model attributes are different that manifest columns for certain values in map
            for manifest_file_key, model_attribute in data_file_paths_map.get('mappings').items():
                path_value = row_paths.get(self.file_ingester.clean_row_keys(manifest_file_key))
                row[model_attribute] = f'gs://{path_value}' if 'gs://' not in path_value else path_value
            return row

        # If file paths not defined them define implicitly
        for file_def in required_file_paths:
            if genome_type == config.GENOME_TYPE_ARRAY:
                if 'idat' in file_def["file_type"]:
                    file_path = f'gs://{gc_bucket}/Genotyping_sample_raw_data/{row["chipwellbarcode"]}' + \
                                f'_{file_def["file_type"]}'
                else:
                    file_path = f'gs://{gc_bucket}/Genotyping_sample_raw_data/{row["chipwellbarcode"]}.' + \
                                f'{file_def["file_type"]}'
                row[file_def['file_path_attribute']] = file_path
            elif genome_type == config.GENOME_TYPE_WGS:
                file_path = f'gs://{gc_bucket}/{genome_center_datafile_prefix_map[site_id][file_def["file_type"]]}/' + \
                            f'{site_id.upper()}_{row["biobankid"]}_{row["sampleid"]}_{row["limsid"]}_1.' + \
                            f'{file_def["file_type"]}'
                row[file_def['file_path_attribute']] = file_path
        return row

    def prep_aw2_row_attributes(self, *, row: dict, member: GenomicSetMember) -> dict:
        """
        Set contamination, contamination category,
        call rate, member_id, and file_id on AW2 row dictionary
        :param member:
        :param row:
        :return: row dictionary or ERROR code
        """
        row['member_id'] = member.id
        row['file_id'] = self.file_ingester.file_obj.id

        # handle mapped reads in case they are longer than field length
        if 'mappedreadspct' in row.keys() and len(row['mappedreadspct']) > 1:
            row['mappedreadspct'] = row['mappedreadspct'][0:10]

        # Set default values in case they upload "" and processing status of "fail"
        row['contamination_category'] = GenomicContaminationCategory.UNSET
        row['contamination_category_str'] = "UNSET"
        # Truncate call rate
        try:
            row['callrate'] = row['callrate'][:10]
        except KeyError:
            pass
        # Convert blank alignedq30bases to none
        try:
            row['alignedq30bases'] = None if row['alignedq30bases'] == '' else row['alignedq30bases']
        except KeyError:
            pass
        # Validate and clean contamination data
        try:
            row['contamination'] = float(row['contamination'])
            # Percentages shouldn't be less than 0
            row['contamination'] = 0 if row['contamination'] < 0 else row['contamination']
        except ValueError:
            if row['processingstatus'].lower() != 'pass':
                return row
            _message = f'{self.file_ingester.controller.job_id.name}: Contamination must be a number for sample_id:' \
                       f' {row["sampleid"]}'
            self.file_ingester.controller.create_incident(source_job_run_id=self.file_ingester.controller.job_run.id,
                                                          source_file_processed_id=self.file_ingester.file_obj.id,
                                                          code=GenomicIncidentCode.DATA_VALIDATION_FAILED.name,
                                                          message=_message,
                                                          biobank_id=member.biobankId,
                                                          sample_id=row['sampleid'],
                                                          )

            return GenomicSubProcessResult.ERROR

        # Calculate contamination_category
        contamination_value = float(row['contamination'])
        category = self.file_ingester.calculate_contamination_category(
            member.collectionTubeId,
            contamination_value,
            member
        )
        row['contamination_category'] = category
        row['contamination_category_str'] = category.name
        return row

    def run_ingestion(self, rows: List[OrderedDict]) -> str:
        """ AW2 ingestion which create/updates a GenomicGCValidationMetrics obj
        :param rows:
        :return result code
        """

        try:
            feedback_dao = GenomicManifestFeedbackDao()
            members_to_update, cleaned_rows = [], [self.file_ingester.clean_row_keys(row) for row in
                                                   rows]

            # All members connected to manifest via sample_ids
            members: List[
                GenomicSetMember] = self.file_ingester.member_dao.get_member_subset_for_metrics_from_sample_ids(
                [obj.get('sampleid') for obj in cleaned_rows]
            )

            # SET pipeline_id
            pipeline_id = cleaned_rows[0].get('pipelineid')
            if not pipeline_id and cleaned_rows[0].get('genometype') in (
                GENOME_TYPE_WGS,
                GENOME_TYPE_WGS_INVESTIGATION
            ):
                pipeline_id = config.GENOMIC_DEPRECATED_WGS_DRAGEN

            # All metrics connected to manifest via member_ids
            exisiting_metrics: List[
                GenomicGCValidationMetrics] = self.file_ingester.metrics_dao.get_bulk_metrics_for_process_update(
                member_ids=[obj.id for obj in members],
                pipeline_id=pipeline_id
            )

            for row in cleaned_rows:
                sample_id, biobank_id = row['sampleid'], row['biobankid']
                row_member = list(filter(lambda x: x.sampleId == sample_id, members))
                if not row_member:
                    if biobank_id[0] in [get_biobank_id_prefix(), 'T']:
                        biobank_id = biobank_id[1:]
                    message = f"{self.file_ingester.controller.job_id.name}: Cannot find genomic set member for " \
                              f"biobank_id, sample_id: " \
                              f"{biobank_id}, {sample_id}"
                    self.file_ingester.controller.create_incident(
                        source_job_run_id=self.file_ingester.controller.job_run.id,
                        source_file_processed_id=self.file_ingester.file_obj.id,
                        code=GenomicIncidentCode.UNABLE_TO_FIND_MEMBER.name,
                        message=message,
                        biobank_id=biobank_id,
                        sample_id=sample_id,
                    )
                    continue

                # MEMBER actions
                row_member = row_member[0]
                current_state = GenomicWorkflowState.GEM_READY \
                    if row['genometype'] == GENOME_TYPE_ARRAY else GenomicWorkflowState.CVL_READY

                member_dict = {
                    'id': row_member.id,
                    'aw2FileProcessedId': self.file_ingester.file_obj.id,
                    'genomicWorkflowState': int(current_state),
                    'genomicWorkflowStateStr': str(current_state),
                    'genomicWorkflowStateModifiedTime': clock.CLOCK.now()
                }
                members_to_update.append(member_dict)

                # METRIC actions
                prepped_row = self.prep_aw2_row_attributes(
                    row=row,
                    member=row_member
                )
                if prepped_row == GenomicSubProcessResult.ERROR:
                    continue
                prepped_row['pipelineid'] = pipeline_id
                # set metric file paths
                prepped_row = self.set_metrics_data_file_paths(
                    genome_type=row['genometype'],
                    row=prepped_row
                )

                # MEMBER REPLATING actions - (conditional) based on existing metric record
                existing_metric_obj: List[GenomicGCValidationMetrics] = list(
                    filter(lambda x: x.genomicSetMemberId == row_member.id, exisiting_metrics)
                )
                metric_id = None if not existing_metric_obj else existing_metric_obj[0].id
                if not metric_id:
                    if row_member.genomeType in [
                        GENOME_TYPE_ARRAY,
                        GENOME_TYPE_WGS
                    ] and prepped_row[
                        'contamination_category'] in [
                        GenomicContaminationCategory.EXTRACT_WGS,
                        GenomicContaminationCategory.EXTRACT_BOTH
                    ]:
                        self.file_ingester.insert_member_for_replating(
                            row_member.id,
                            prepped_row['contamination_category']
                        )

                # UPSERT cloud task for current metric
                prepped_row['contamination_category'] = int(prepped_row['contamination_category'])
                self.file_ingester.controller.execute_cloud_task({
                    'metric_id': metric_id,
                    'payload_dict': prepped_row,
                }, 'genomic_gc_metrics_upsert')

                # MANIFEST/FEEDBACK actions - (conditional) based on existing manifest record
                manifest_file = self.file_ingester.file_processed_dao.get(row_member.aw1FileProcessedId)
                if manifest_file and not metric_id:
                    feedback_dao.increment_feedback_count(manifest_file.genomicManifestFileId)

            # BULK Update for ALL members
            self.file_ingester.member_dao.bulk_update(members_to_update)

            return GenomicSubProcessResult.SUCCESS

        except Exception as e:
            logging.warning(f'Error when ingesting AW2 manifest file: {self.file_ingester.file_obj.filePath}: {e}')
            return GenomicSubProcessResult.ERROR


class GenomicAW4Workflow(BaseGenomicShortReadWorkflow):

    def get_gc_data(self):
        ...

    @classmethod
    def get_qc_status_from_value(cls, *, value_to_check):
        """
        Returns the GenomicQcStatus enum value for
        :param value_to_check: string from AW4 file (PASS/FAIL)
        :return: GenomicQcStatus
        """
        if value_to_check.strip().lower() == 'pass':
            return GenomicQcStatus.PASS
        elif value_to_check.strip().lower() == 'fail':
            return GenomicQcStatus.FAIL
        else:
            logging.warning(f'Value from AW4 "{value_to_check}" is not PASS/FAIL.')
            return GenomicQcStatus.UNSET

    def run_ingestion(self, rows: List[OrderedDict]) -> str:
        """
        AW4 ingestion updates GenomicSetMember and GenomicGCValidationMetrics objs
        :param rows:
        :return result code
        """
        try:
            cleaned_rows: List[dict] = [self.file_ingester.clean_row_keys(row) for row in rows]
            sample_ids = [obj.get('sampleid') for obj in cleaned_rows]
            pipeline_id: str = cleaned_rows[0].get('pipelineid')

            members: List = self.file_ingester.member_dao.get_member_ids_from_aw3_sample_ids(sample_ids=sample_ids)
            metrics: List = self.file_ingester.metrics_dao.get_metrics_by_member_ids(
                member_ids=[obj.id for obj in members],
                pipeline_id=pipeline_id
            )

            updated_members, updated_metrics = [], []

            for member in members:
                # call enrollment cloud task
                self.file_ingester.controller.execute_cloud_task(
                    endpoint='update_enrollment_status',
                    payload={
                        'participant_id': member.participantId
                    },
                    task_queue='resource-tasks'
                )

                # get row from list
                row_copy = list(filter(lambda x: x.get('sampleid') == member.sampleId, cleaned_rows))[0]

                # build/append member dict
                qc_status = self.get_qc_status_from_value(value_to_check=row_copy.get('qcstatus'))
                updated_members.append({
                    'id': member.id,
                    'aw4ManifestJobRunID': self.file_ingester.controller.job_run.id,
                    'qcStatus': qc_status,
                    'qcStatusStr': qc_status.name
                })

                # get metrics from list
                current_metric = list(filter(lambda x: x.genomicSetMemberId == member.id, metrics))

                # build/append metric dict
                if current_metric:
                    current_metric = current_metric[0]

                    metrics_dict = {
                        'id': current_metric.id,
                        'drcSexConcordance': row_copy.get('drcsexconcordance'),
                        'aw4ManifestJobRunID': self.file_ingester.controller.job_run.id
                    }
                    if self.file_ingester.controller.job_id == GenomicJob.AW4_ARRAY_WORKFLOW:
                        metrics_dict.update({'drcCallRate': row_copy.get('drccallrate')})
                    elif self.file_ingester.controller.job_id == GenomicJob.AW4_WGS_WORKFLOW:
                        metrics_dict.update(
                            {
                                'drcContamination': row_copy.get('drccontamination'),
                                'drcMeanCoverage': row_copy.get('drcmeancoverage'),
                                'drcFpConcordance': row_copy.get('drcfpconcordance'),
                            })
                    updated_metrics.append(metrics_dict)

            # end with bulk update
            self.file_ingester.member_dao.bulk_update(updated_members)
            if updated_metrics:
                self.file_ingester.metrics_dao.bulk_update(updated_metrics)

            return GenomicSubProcessResult.SUCCESS

        except Exception as e:
            logging.warning(f'Error when ingesting AW4 manifest file: {self.file_ingester.file_obj.filePath}: {e}')
            return GenomicSubProcessResult.ERROR
