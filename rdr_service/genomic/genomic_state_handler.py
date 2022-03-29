import abc

from rdr_service.genomic_enums import GenomicWorkflowState


class GenomicStateBase:
    """Abstract base class for genomic states"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def transition_function(self, signal):
        return


class IgnoreState(GenomicStateBase):
    """
    Ignore State, used to effectively remove GenomicSetMembers from
    the genomics system.
    """
    def transition_function(self, signal):
        return GenomicWorkflowState.IGNORE


class ControlSampleState(GenomicStateBase):
    """
    Control Sample State, used to mark programmatic control samples,
     for example NIST samples.
    """
    def transition_function(self, signal):
        return GenomicWorkflowState.CONTROL_SAMPLE


class AW0ReadyState(GenomicStateBase):
    """
    State representing new Genomic Set Members
    ready for AW0 manifest state
    """
    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.AW0

        return GenomicWorkflowState.AW0_READY


class AW0State(GenomicStateBase):
    """State representing the AW0 manifest state"""
    def transition_function(self, signal):
        if signal == 'aw1-reconciled':
            return GenomicWorkflowState.AW1

        elif signal == 'aw1-failed':
            return GenomicWorkflowState.AW1F_PRE

        return GenomicWorkflowState.AW0


class AW1State(GenomicStateBase):
    """State representing the AW1 manifest state"""
    def transition_function(self, signal):
        if signal == 'aw1-failed':
            return GenomicWorkflowState.AW1F_POST

        elif signal == 'aw2':
            return GenomicWorkflowState.AW2

        return GenomicWorkflowState.AW1


class AW2State(GenomicStateBase):
    """State representing the AW2 manifest state"""
    def transition_function(self, signal):
        if signal == 'missing':
            return GenomicWorkflowState.GC_DATA_FILES_MISSING

        elif signal == 'fail':
            return GenomicWorkflowState.AW2_FAIL

        elif signal == 'cvl-ready':
            return GenomicWorkflowState.CVL_READY

        elif signal == 'gem-ready':
            return GenomicWorkflowState.GEM_READY

        return GenomicWorkflowState.AW2


class AW2MissingState(GenomicStateBase):
    """State representing the AW2 Missing Data state"""

    def transition_function(self, signal):
        if signal == 'missing':
            return GenomicWorkflowState.GC_DATA_FILES_MISSING

        elif signal == 'cvl-ready':
            return GenomicWorkflowState.CVL_READY

        elif signal == 'gem-ready':
            return GenomicWorkflowState.GEM_READY

        return GenomicWorkflowState.GC_DATA_FILES_MISSING


class GEMReadyState(GenomicStateBase):
    """State representing the GEM_READY state"""
    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.A1

        return GenomicWorkflowState.GEM_READY


class A1State(GenomicStateBase):
    """State representing the A1 manifest state"""
    def transition_function(self, signal):
        if signal == 'a2-gem-pass':
            return GenomicWorkflowState.GEM_RPT_READY

        if signal == 'a2-gem-fail':
            return GenomicWorkflowState.A2F

        if signal == 'unconsented':
            return GenomicWorkflowState.GEM_RPT_PENDING_DELETE

        return GenomicWorkflowState.A1


class A2PassState(GenomicStateBase):
    """State representing the A2 manifest state"""

    def transition_function(self, signal):
        if signal == 'report-ready':
            return GenomicWorkflowState.GEM_RPT_READY

        if signal == 'unconsented':
            return GenomicWorkflowState.GEM_RPT_PENDING_DELETE

        return GenomicWorkflowState.A2


class A2FailState(GenomicStateBase):
    """State representing the A2 manifest GEM failure state"""

    def transition_function(self, signal):
        if signal == 'report-ready':
            return GenomicWorkflowState.GEM_RPT_READY

        if signal == 'unconsented':
            return GenomicWorkflowState.GEM_RPT_PENDING_DELETE

        return GenomicWorkflowState.A2F


class A3State(GenomicStateBase):
    """State representing the A3 manifest; GEM Delete states"""

    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.GEM_RPT_DELETED

        return GenomicWorkflowState.A3


class GEMReportReady(GenomicStateBase):
    """State representing the GEM Report"""

    def transition_function(self, signal):
        if signal == 'unconsented':
            return GenomicWorkflowState.GEM_RPT_PENDING_DELETE

        return GenomicWorkflowState.GEM_RPT_READY


class GEMReportPendingDelete(GenomicStateBase):
    """State representing when Consent revoked, input to A3 Manifest"""

    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.GEM_RPT_DELETED

        if signal == 'reconsented':
            return GenomicWorkflowState.GEM_READY

        return GenomicWorkflowState.GEM_RPT_PENDING_DELETE


class GEMReportDeleted(GenomicStateBase):
    """State representing when Consent revoked, input to A3 Manifest"""

    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.GEM_RPT_DELETED

        if signal == 'reconsented':
            return GenomicWorkflowState.GEM_READY

        return GenomicWorkflowState.GEM_RPT_DELETED


class CVLReadyState(GenomicStateBase):
    """State representing the CVL_READY state"""
    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.CVL_W1IL

        if signal == 'unconsented':
            return GenomicWorkflowState.CVL_RPT_PENDING_DELETE

        return GenomicWorkflowState.CVL_READY


class W1State(GenomicStateBase):
    """State representing the W1 manifest state"""
    def transition_function(self, signal):
        if signal == 'w2-ingestion-success':
            return GenomicWorkflowState.W2


class W2State(GenomicStateBase):
    """State representing the W2 manifest state"""
    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.W3


class W3State(GenomicStateBase):
    """State representing the W3 manifest state"""
    def transition_function(self, signal):
        if signal == 'aw1c-reconciled':
            return GenomicWorkflowState.AW1C

        if signal == 'aw1c-failed':
            # TODO: There may be a pre-accessioning state as well
            return GenomicWorkflowState.AW1CF_POST


class W2SCState(GenomicStateBase):
    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.CVL_W3SR


class GenomicStateHandler:
    """
    Basic FSM for Genomic States. Returns call to state's transition_function()
    """
    states = {
        GenomicWorkflowState.IGNORE: IgnoreState(),
        GenomicWorkflowState.CONTROL_SAMPLE: ControlSampleState(),
        GenomicWorkflowState.AW0_READY: AW0ReadyState(),
        GenomicWorkflowState.AW0: AW0State(),
        GenomicWorkflowState.AW1: AW1State(),
        GenomicWorkflowState.AW2: AW2State(),
        GenomicWorkflowState.GC_DATA_FILES_MISSING: AW2MissingState(),
        GenomicWorkflowState.CVL_READY: CVLReadyState(),
        GenomicWorkflowState.W1: W1State(),
        GenomicWorkflowState.W2: W2State(),
        GenomicWorkflowState.W3: W3State(),
        GenomicWorkflowState.GEM_READY: GEMReadyState(),
        GenomicWorkflowState.A1: A1State(),
        GenomicWorkflowState.A2: A2PassState(),
        GenomicWorkflowState.A2F: A2FailState(),
        GenomicWorkflowState.A3: A3State(),
        GenomicWorkflowState.GEM_RPT_READY: GEMReportReady(),
        GenomicWorkflowState.GEM_RPT_PENDING_DELETE: GEMReportPendingDelete(),
        GenomicWorkflowState.GEM_RPT_DELETED: GEMReportDeleted(),
        # Replating is functionally equivalent to AW0
        GenomicWorkflowState.EXTRACT_REQUESTED: AW0State(),
        GenomicWorkflowState.CVL_W2SC: W2SCState(),
    }

    @classmethod
    def get_new_state(cls, current_state, signal):
        _state = cls.states.get(current_state, None)
        if _state:
            return _state.transition_function(signal)

        return



