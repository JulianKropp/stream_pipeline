from dataclasses import dataclass, field
import pickle
from typing import Any, List, Union
import uuid

from . import data_pb2
from .thread_safe_class import ThreadSafeClass
from .error import Error, exception_to_error


@dataclass
class DataPackageModule(ThreadSafeClass):
    """
    Represents metadata for a module that has processed a data package.
    
    Attributes:
        id (str):                               ID of the module.
        running (bool):                         Indicates if the module is currently running.
        start_time (float):                     Timestamp when the module started.
        end_time (float):                       Timestamp when the module finished.
        waiting_time (float):                   Time spent waiting for the mutex to unlock.
        processing_time (float):                Time spent processing the data package.
        total_time (float):                     Total time spent on the data package processing.
        sub_modules (List[DataPackageModule]):  List of sub-modules that processed the data package.
        message (str):                          Informational message.
        success (bool):                         Indicates if the processing was successful.
        error (Union[Exception, Error, None]):  Error encountered during processing, if any.
    """
    id: str = field(default_factory=lambda: "Module-" + str(uuid.uuid4()))
    running: bool = False
    start_time: float = 0.0
    end_time: float = 0.0
    waiting_time: float = 0.0
    processing_time: float = 0.0
    total_time: float = 0.0
    sub_modules: List['DataPackageModule'] = field(default_factory=list)
    message: str = ""
    success: bool = True
    error: Union[Error, None] = None

    def set_from_grpc(self, grpc_module: data_pb2.DataPackageModule) -> None:
        """
        Updates the current instance with data from a gRPC module.
        """
        temp_immutable_attributes = self._immutable_attributes
        self._immutable_attributes = []

        self.id = grpc_module.id
        self.running = grpc_module.running
        self.start_time = grpc_module.start_time
        self.end_time = grpc_module.end_time
        self.waiting_time = grpc_module.waiting_time
        self.processing_time = grpc_module.processing_time
        self.total_time = grpc_module.total_time

        existing_sub_modules = {sub_module.id: sub_module for sub_module in self.sub_modules}
        for module in grpc_module.sub_modules:
            if module:
                if module.id in existing_sub_modules:
                    existing_sub_modules[module.id].set_from_grpc(module)
                else:
                    new_sub_module = DataPackageModule()
                    new_sub_module.set_from_grpc(module)
                    self.sub_modules.append(new_sub_module)

        self.message = grpc_module.message
        self.success = grpc_module.success
        if grpc_module.error and grpc_module.error.ListFields():
            if self.error is None:
                self.error = Error()
            self.error.set_from_grpc(grpc_module.error)
        else:
            self.error = None
        
        self._immutable_attributes = temp_immutable_attributes

    def to_grpc(self) -> data_pb2.DataPackageModule:
        """
        Converts the current instance to a gRPC module.
        """
        grpc_module = data_pb2.DataPackageModule()
        grpc_module.id = self.id
        grpc_module.running = self.running
        grpc_module.start_time = self.start_time
        grpc_module.end_time = self.end_time
        grpc_module.waiting_time = self.waiting_time
        grpc_module.processing_time = self.processing_time
        grpc_module.total_time = self.total_time
        grpc_module.sub_modules.extend([module.to_grpc() for module in self.sub_modules])
        grpc_module.message = self.message
        grpc_module.success = self.success
        if isinstance(self.error, Exception):
            self.error = exception_to_error(self.error)
        if self.error:
            grpc_module.error.CopyFrom(self.error.to_grpc())
        else:
            grpc_module.error.Clear()
        return grpc_module


@dataclass
class DataPackagePhase(ThreadSafeClass):
    """
    Represents metadata for a phase that has processed a data package.
    
    Attributes:
        id (str):                           ID of the phase.
        running (bool):                     Indicates if the phase is currently running.
        start_time (float):                 Timestamp when the phase started.
        end_time (float):                   Timestamp when the phase finished.
        processing_time (float):            Time spent processing the data package.
        modules (List[DataPackageModule]):  List of modules that processed the data package.
    """
    id: str = field(default_factory=lambda: "Phase-" + str(uuid.uuid4()))
    running: bool = False
    start_time: float = 0.0
    end_time: float = 0.0
    processing_time: float = 0.0
    modules: List['DataPackageModule'] = field(default_factory=list)

    def set_from_grpc(self, grpc_phase: data_pb2.DataPackagePhase) -> None:
        """
        Updates the current instance with data from a gRPC phase.
        """
        temp_immutable_attributes = self._immutable_attributes
        self._immutable_attributes = []

        self.id = grpc_phase.id
        self.running = grpc_phase.running
        self.start_time = grpc_phase.start_time
        self.end_time = grpc_phase.end_time
        self.processing_time = grpc_phase.processing_time

        existing_modules = {module.id: module for module in self.modules}
        for module in grpc_phase.modules:
            if module:
                if module.id in existing_modules:
                    existing_modules[module.id].set_from_grpc(module)
                else:
                    new_module = DataPackageModule()
                    new_module.set_from_grpc(module)
                    self.modules.append(new_module)

        self._immutable_attributes = temp_immutable_attributes

    def to_grpc(self) -> data_pb2.DataPackagePhase:
        """
        Converts the current instance to a gRPC phase.
        """
        grpc_phase = data_pb2.DataPackagePhase()
        grpc_phase.id = self.id
        grpc_phase.running = self.running
        grpc_phase.start_time = self.start_time
        grpc_phase.end_time = self.end_time
        grpc_phase.processing_time = self.processing_time
        grpc_phase.modules.extend([module.to_grpc() for module in self.modules])
        return grpc_phase


@dataclass
class DataPackagePhaseController(ThreadSafeClass):
    """
    Represents metadata for a phase execution that has processed a data package.
    
    Attributes:
        id (str):                           ID of the phase controller.
        mode (str):                         Type of phase execution (e.g., NOT_PARALLEL, ORDER_BY_SEQUENCE, FIRST_WINS, NO_ORDER).
        workers (int):                      Number of workers used for executing phases in parallel. 0 means no multi-threading.
        sequence_number (int):              Sequence number of the data package.
        running (bool):                     Indicates if the phase execution is currently running.
        start_time (float):                 Timestamp when the phase execution started.
        end_time (float):                   Timestamp when the phase execution finished.
        waiting_time (float):               Time spent waiting for the thread pool to unlock.
        processing_time (float):            Time spent processing the phases.
        total_time (float):                 Total time spent on phase execution.
        phases (List[DataPackagePhase]):    List of phases that processed the data package.
    """
    id: str = field(default_factory=lambda: "Controller-" + str(uuid.uuid4()))
    mode: str = "NOT_PARALLEL"
    workers: int = 1
    sequence_number: int = -1
    running: bool = False
    start_time: float = 0.0
    end_time: float = 0.0
    waiting_time: float = 0.0
    processing_time: float = 0.0
    total_time: float = 0.0
    phases: List[DataPackagePhase] = field(default_factory=list)

    def set_from_grpc(self, grpc_execution: data_pb2.DataPackagePhaseController) -> None:
        """
        Updates the current instance with data from a gRPC phase execution.
        """
        temp_immutable_attributes = self._immutable_attributes
        self._immutable_attributes = []

        self.id = grpc_execution.id
        self.mode = grpc_execution.mode
        self.workers = grpc_execution.workers
        self.sequence_number = grpc_execution.sequence_number
        self.running = grpc_execution.running
        self.start_time = grpc_execution.start_time
        self.end_time = grpc_execution.end_time
        self.waiting_time = grpc_execution.waiting_time
        self.processing_time = grpc_execution.processing_time
        self.total_time = grpc_execution.total_time

        existing_phases = {phase.id: phase for phase in self.phases}
        for phase in grpc_execution.phases:
            if phase:
                if phase.id in existing_phases:
                    existing_phases[phase.id].set_from_grpc(phase)
                else:
                    new_phase = DataPackagePhase()
                    new_phase.set_from_grpc(phase)
                    self.phases.append(new_phase)

        self._immutable_attributes = temp_immutable_attributes

    def to_grpc(self) -> data_pb2.DataPackagePhaseController:
        """
        Converts the current instance to a gRPC phase execution.
        """
        grpc_execution = data_pb2.DataPackagePhaseController()
        grpc_execution.id = self.id
        grpc_execution.mode = self.mode
        grpc_execution.workers = self.workers
        grpc_execution.sequence_number = self.sequence_number
        grpc_execution.running = self.running
        grpc_execution.start_time = self.start_time
        grpc_execution.end_time = self.end_time
        grpc_execution.waiting_time = self.waiting_time
        grpc_execution.processing_time = self.processing_time
        grpc_execution.total_time = self.total_time
        grpc_execution.phases.extend([phase.to_grpc() for phase in self.phases])
        return grpc_execution


@dataclass
class DataPackage(ThreadSafeClass):
    """
    Represents the data and metadata for a pipeline process, passed through the pipeline and between modules.
    
    Attributes:
        id (str):                                       Unique identifier for the data package. Immutable.
        pipeline_id (str):                              ID of the pipeline handling this package.
        pipeline_instance_id (str):                     ID of the pipeline instance handling this package.
        controller (List[DataPackagePhaseController]):  List of phases processed in the mode of execution.
        data (Any):                                     Actual data contained in the package.
        running (bool):                                 Indicates if the data package is currently being processed.
        start_time (float):                             Timestamp when the data package started processing.
        end_time (float):                               Timestamp when the data package finished processing.
        total_waiting_time (float):                     Total time spent waiting for the mutex to unlock.
        total_processing_time (float):                  Total time spent processing the data package.
        total_time (float):                             Total time spent processing the data package.
        success (bool):                                 Indicates if the process was successful.
        errors (List[Union[Error, Exception, None]]):   List of errors that occurred during processing.
    """
    id: str = field(default_factory=lambda: "DP-" + str(uuid.uuid4()), init=False)
    pipeline_id: str = ""
    pipeline_instance_id: str = ""
    controller: List[DataPackagePhaseController] = field(default_factory=list)
    data: Any = None
    running: bool = False
    start_time: float = 0.0
    end_time: float = 0.0
    total_waiting_time: float = 0.0
    total_processing_time: float = 0.0
    total_time: float = 0.0
    success: bool = True
    errors: List[Union[Error, None]] = field(default_factory=list)

    # Immutable attributes
    _immutable_attributes: List[str] = field(default_factory=lambda: ['id', 'pipeline_id'])

    def set_from_grpc(self, grpc_package: data_pb2.DataPackage) -> None:
        """
        Updates the current instance with data from a gRPC data package.
        """
        temp_immutable_attributes = self._immutable_attributes
        self._immutable_attributes = []

        self.id = grpc_package.id
        self.pipeline_id = grpc_package.pipeline_id
        self.pipeline_instance_id = grpc_package.pipeline_instance_id

        existing_phases = {phase.id: phase for phase in self.controller}
        for execution in grpc_package.controller:
            if execution:
                if execution.id in existing_phases:
                    existing_phases[execution.id].set_from_grpc(execution)
                else:
                    new_execution = DataPackagePhaseController()
                    new_execution.set_from_grpc(execution)
                    self.controller.append(new_execution)

        self.data = pickle.loads(grpc_package.data)
        self.running = grpc_package.running
        self.start_time = grpc_package.start_time
        self.end_time = grpc_package.end_time
        self.total_waiting_time = grpc_package.total_waiting_time
        self.total_processing_time = grpc_package.total_processing_time
        self.total_time = grpc_package.total_time
        self.success = grpc_package.success

        existing_errors = {error.id: error for error in self.errors if error}
        for error in grpc_package.errors:
            if error:
                if error.id in existing_errors:
                    existing_errors[error.id].set_from_grpc(error)
                else:
                    new_error = Error()
                    new_error.set_from_grpc(error)
                    self.errors.append(new_error)

        self._immutable_attributes = temp_immutable_attributes

    def to_grpc(self) -> data_pb2.DataPackage:
        """
        Converts the current instance to a gRPC data package.
        """
        grpc_package = data_pb2.DataPackage()
        grpc_package.id = self.id
        grpc_package.pipeline_id = self.pipeline_id
        grpc_package.pipeline_instance_id = self.pipeline_instance_id
        grpc_package.controller.extend([phase.to_grpc() for phase in self.controller])
        grpc_package.data = pickle.dumps(self.data)
        grpc_package.running = self.running
        grpc_package.start_time = self.start_time
        grpc_package.end_time = self.end_time
        grpc_package.total_waiting_time = self.total_waiting_time
        grpc_package.total_processing_time = self.total_processing_time
        grpc_package.total_time = self.total_time
        grpc_package.success = self.success
        grpc_package.errors.extend([error.to_grpc() for error in self.errors if error])
        return grpc_package