import abc

from rdr_service.participant_enums import GenomicWorkflowState


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
        return GenomicWorkflowState.IGNORE


class AW0ReadyState(GenomicStateBase):
    """
    State representing new Genomic Set Members
    ready for AW0 manifest state
    """
    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.AW0


class AW0State(GenomicStateBase):
    """State representing the AW0 manifest state"""
    def transition_function(self, signal):
        if signal == 'aw1-reconciled':
            return GenomicWorkflowState.AW1


class AW1State(GenomicStateBase):
    """State representing the AW1 manifest state"""
    def transition_function(self, signal):
        # TODO: this will be updated to appropriate states in a future PR
        if signal == 'aw1-failed':
            return GenomicWorkflowState.AW1F_POST


class AW2State(GenomicStateBase):
    """State representing the AW2 manifest state"""
    def transition_function(self, signal):
        if signal == 'missing':
            return GenomicWorkflowState.AW2_MISSING

        elif signal == 'fail':
            return GenomicWorkflowState.AW2_FAIL

        elif signal == 'cvl-ready':
            return GenomicWorkflowState.CVL_READY

        elif signal == 'gem-ready':
            return GenomicWorkflowState.GEM_READY


class GEMReadyState(GenomicStateBase):
    """State representing the GEM_READY state"""
    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.A1


class A1State(GenomicStateBase):
    """State representing the A1 manifest state"""
    def transition_function(self, signal):
        if signal == 'a2-gem-pass':
            return GenomicWorkflowState.GEM_RPT_READY

        if signal == 'a2-gem-fail':
            return GenomicWorkflowState.A2F

        if signal == 'unconsented':
            return GenomicWorkflowState.GEM_RPT_PENDING_DELETE


class A2PassState(GenomicStateBase):
    """State representing the A2 manifest state"""

    def transition_function(self, signal):
        if signal == 'report-ready':
            return GenomicWorkflowState.GEM_RPT_READY

        if signal == 'unconsented':
            return GenomicWorkflowState.GEM_RPT_PENDING_DELETE


class A2FailState(GenomicStateBase):
    """State representing the A2 manifest GEM failure state"""

    def transition_function(self, signal):
        if signal == 'report-ready':
            return GenomicWorkflowState.GEM_RPT_READY

        if signal == 'unconsented':
            return GenomicWorkflowState.GEM_RPT_PENDING_DELETE


class A3State(GenomicStateBase):
    """State representing the A3 manifest; GEM Delete states"""

    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.GEM_RPT_DELETED


class GEMReportReady(GenomicStateBase):
    """State representing the GEM Report"""

    def transition_function(self, signal):
        if signal == 'unconsented':
            return GenomicWorkflowState.GEM_RPT_PENDING_DELETE


class GEMReportPendingDelete(GenomicStateBase):
    """State representing when Consent revoked, input to A3 Manifest"""

    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.GEM_RPT_DELETED

        if signal == 'reconsented':
            return GenomicWorkflowState.GEM_READY


class GEMReportDeleted(GenomicStateBase):
    """State representing when Consent revoked, input to A3 Manifest"""

    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.GEM_RPT_DELETED

        if signal == 'reconsented':
            return GenomicWorkflowState.GEM_READY


class CVLReadyState(GenomicStateBase):
    """State representing the CVL_READY state"""
    def transition_function(self, signal):
        if signal == 'manifest-generated':
            return GenomicWorkflowState.W1


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


class GenomicStateHandler:
    """
    Basic FSM for Genomic States. Returns call to state's transision_function()
    """
    states = {
        GenomicWorkflowState.IGNORE: IgnoreState(),
        GenomicWorkflowState.CONTROL_SAMPLE: ControlSampleState(),
        GenomicWorkflowState.AW0_READY: AW0ReadyState(),
        GenomicWorkflowState.AW0: AW0State(),
        GenomicWorkflowState.AW1: AW1State(),
        GenomicWorkflowState.AW2: AW2State(),
        GenomicWorkflowState.CVL_READY: CVLReadyState(),
        GenomicWorkflowState.W1: W1State(),
        GenomicWorkflowState.W2: W2State(),
        GenomicWorkflowState.GEM_READY: GEMReadyState(),
        GenomicWorkflowState.A1: A1State(),
        GenomicWorkflowState.A2: A2PassState(),
        GenomicWorkflowState.A2F: A2FailState(),
        GenomicWorkflowState.A3: A3State(),
        GenomicWorkflowState.GEM_RPT_READY: GEMReportReady(),
        GenomicWorkflowState.GEM_RPT_PENDING_DELETE: GEMReportPendingDelete(),
        GenomicWorkflowState.GEM_RPT_DELETED: GEMReportDeleted(),
    }

    @classmethod
    def get_new_state(cls, current_state, signal):
        _state = cls.states.get(current_state, None)

        if _state is not None:
            return _state.transition_function(signal)

        return



