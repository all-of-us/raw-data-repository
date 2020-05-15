import abc

from rdr_service.participant_enums import GenomicWorkflowState


class GenomicStateBase:
    """Abstract base class for genomic states"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def transition_function(self, signal):
        return


class AW2State(GenomicStateBase):
    """State representing the AW2 manifest state"""
    def transition_function(self, signal):
        if signal == 'missing':
            return GenomicWorkflowState.AW2_MISSING

        elif signal == 'fail':
            return GenomicWorkflowState.AW2_FAIL

        elif signal == 'cvl-ready':
            return GenomicWorkflowState.CVL_READY


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


class GenomicStateHandler:
    """
    Basic FSM for Genomic States. Returns call to state's transision_function()
    """
    states = {
        GenomicWorkflowState.AW2: AW2State(),
        GenomicWorkflowState.CVL_READY: CVLReadyState(),
        GenomicWorkflowState.W1: W1State(),
    }

    @classmethod
    def get_new_state(cls, current_state, signal):
        _state = cls.states.get(current_state, None)

        if _state is not None:
            return _state.transition_function(signal)

        return



