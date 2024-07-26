import pickle
from dataclasses import dataclass, field, asdict
import json
import types
from typing import Any, Dict, List, Optional, Tuple, Union
import threading
import uuid
import copy
import time

from .error import json_error_handler_dict, Error

@dataclass
class DataPackageModule:
    """
    Class which contains metadata for a module that has processed a data package.
    Attributes:
        module_id (str):            ID of the module.
        start_time (float):         Time when the module was started.
        end_time (float):           Time when the module finished.
        waiting_time (float):       Time spent waiting for the mutex to unlock.
        processing_time (float):    Time spent processing the data package.
        total_time (float):         Total time spent processing the data package.
        success (bool):             Indicates if the process was successful.
        error (Exception or Error): Contains the error. Can be set as type Exception or Error and will be converted to Error.
    """
    module_id: str
    start_time: float
    end_time: float
    waiting_time: float
    processing_time: float
    total_time: float
    success: bool
    error: Union[Exception, Error, None] = None



@dataclass
class DataPackage:
    """
    Class which contains the data and metadata for a pipeline process and will be passed through the pipeline and between modules.
    Attributes:
        id (str) immutable:                     Unique identifier for the data package.
        pipeline_id (str):                      ID of the pipeline handling this package.
        pipeline_executer_id (str) immutable:   ID of the pipeline executor handling this package.
        sequence_number (int):                  The sequence number of the data package.
        modules (List[DataPackageModule]):      List of modules that have processed the data package. Including measurements.
        data (Any):                             The actual data contained in the package.
        success (bool):                         Indicates if the process was successful. If not successful, the error attribute should be set.
        message (str):                          Info message.
        error (Error):                          Error message if the process was not successful.
    """
    pipeline_id: str
    pipeline_executer_id: str
    sequence_number: int
    id: str = ""
    modules: List[DataPackageModule] = field(default_factory=list)  # Replace Any with DataPackageModule if defined
    data: Any = None
    running: bool = False
    success: bool = True
    message: str = ""
    error: Optional[Union[Error, Exception, None]] = None

    # Immutable attributes
    _immutable_attributes: List[str] = field(default_factory=lambda: 
                                            [
                                                'id',
                                                'pipeline_id',
                                                'pipeline_executer_id',
                                            ]
                                        )

    # Mutexes for thread-safe property access
    _mutexes: Dict[str, threading.Lock] = field(default_factory=dict, init=False)

    _mutex: threading.Lock = threading.Lock() # For locking all properties starts with _

    def __getattribute__(self, name: str) -> Any:
        if name == '_mutex':
            return super().__getattribute__(name)

        if name.startswith('_'): 
            with self._mutex:
                return super().__getattribute__(name)
        attr = super().__getattribute__(name)
        if isinstance(attr, types.MethodType):
            return attr

        if '_mutexes' in self.__dict__:
            if name not in self._mutexes:
                self._mutexes[name] = threading.Lock()

            with self._mutexes[name]:
                return super().__getattribute__(name)
        else:
            return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any):
        if name.startswith('_'):
            with self._mutex:
                super().__setattr__(name, value)
                return
        
        if '_immutable_attributes' in self.__dict__:
            for attr in self._immutable_attributes:
                if name == attr and attr in self.__dict__:
                    raise AttributeError(f"'{self.__class__.__name__}' object attribute '{attr}' is immutable")

        if '_mutexes' in self.__dict__:
            if name not in self._mutexes:
                self._mutexes[name] = threading.Lock()
            with self._mutexes[name]:
                current_thread = threading.current_thread()
                if hasattr(current_thread, 'timed_out') and current_thread.timed_out:
                    raise RuntimeError("Modification not allowed: the thread handling this DataPackage has timed out.")
                super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    def __deepcopy__(self, memo):
        # Create a deep copy of the object without the _mutexes
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        for k, v in self.__dict__.items():
            if k != '_mutexes':
                setattr(result, k, copy.deepcopy(v, memo))

        # Reinitialize the _mutexes after copying
        result._mutexes = {k: threading.Lock() for k in self.__dict__.keys() if k != '_mutexes'}

        return result

    def copy(self):
        # Create a deep copy using the custom __deepcopy__ method
        return copy.deepcopy(self)

    def __str__(self):
        data_dict = {key: value for key, value in self.__dict__.items() if not key.startswith('_')}
        data_dict['error'] = json_error_handler_dict(self.error) if self.error else None
        data_dict['modules'] = [
            {**asdict(module), 'error': json_error_handler_dict(module.error) if module.error else None} 
            for module in data_dict['modules']
        ]

        # Handle non-serializable data
        try:
            jsonstring = json.dumps(data_dict, default=str, indent=4)
        except (TypeError, ValueError):
            data_dict['data'] = "Data which cannot be displayed as JSON"
            jsonstring = json.dumps(data_dict, default=str, indent=4)

        return jsonstring



def grpc_to_normal(grpc_data_package, data_package: Union[None, DataPackage] = None) -> DataPackage:
    error = grpc_data_package.error
    normal_error = Error(
        type=error.type,
        message=error.message,
        traceback=list(error.traceback),
        thread=error.thread,
        start_context=error.start_context,
        thread_id=error.thread_id,
        is_daemon=error.is_daemon,
        local_vars=dict(error.local_vars),
        global_vars=dict(error.global_vars),
        environment_vars=dict(error.environment_vars),
        module_versions=dict(error.module_versions)
    ) if error else None

    modules = [
        DataPackageModule(
            module_id=module.module_id,
            start_time=module.start_time,
            end_time=module.end_time,
            waiting_time=module.waiting_time,
            processing_time=module.processing_time,
            total_time=module.total_time,
            success=module.success,
            error=Error(
                type=module.error.type,
                message=module.error.message,
                traceback=list(module.error.traceback),
                thread=module.error.thread,
                start_context=module.error.start_context,
                thread_id=module.error.thread_id,
                is_daemon=module.error.is_daemon,
                local_vars=dict(module.error.local_vars),
                global_vars=dict(module.error.global_vars),
                environment_vars=dict(module.error.environment_vars),
                module_versions=dict(module.error.module_versions)
            ) if module.error else None
        ) for module in grpc_data_package.modules
    ]

    if isinstance(data_package, DataPackage):
        data_package.modules = modules
        data_package.data = pickle.loads(grpc_data_package.data)
        data_package.running = grpc_data_package.running
        data_package.success = grpc_data_package.success
        data_package.message = grpc_data_package.message
        data_package.error = normal_error
        data_return = data_package
    else:
        data_return = DataPackage(
            id=grpc_data_package.id,
            pipeline_id=grpc_data_package.pipeline_id,
            pipeline_executer_id=grpc_data_package.pipeline_executer_id,
            sequence_number=grpc_data_package.sequence_number,
            modules=modules,
            data=pickle.loads(grpc_data_package.data),
            running=grpc_data_package.running,
            success=grpc_data_package.success,
            message=grpc_data_package.message,
            error=normal_error
        )
    return data_return

def normal_to_grpc(normal_data_package, grpc_data_package_class, grpc_data_package_module_class, grpc_error_class):
    normal_error = normal_data_package.error
    grpc_error = grpc_error_class(
        type=normal_error.type,
        message=normal_error.message,
        traceback=normal_error.traceback,
        thread=normal_error.thread,
        start_context=normal_error.start_context,
        thread_id=normal_error.thread_id,
        is_daemon=normal_error.is_daemon,
        local_vars=normal_error.local_vars,
        global_vars=normal_error.global_vars,
        environment_vars=normal_error.environment_vars,
        module_versions=normal_error.module_versions
    ) if normal_error else None

    modules = [
        grpc_data_package_module_class(
            module_id=module.module_id,
            start_time=module.start_time,
            end_time=module.end_time,
            waiting_time=module.waiting_time,
            processing_time=module.processing_time,
            total_time=module.total_time,
            success=module.success,
            error=grpc_error_class(
                type=module.error.type,
                message=module.error.message,
                traceback=module.error.traceback,
                thread=module.error.thread,
                start_context=module.error.start_context,
                thread_id=module.error.thread_id,
                is_daemon=module.error.is_daemon,
                local_vars=module.error.local_vars,
                global_vars=module.error.global_vars,
                environment_vars=module.error.environment_vars,
                module_versions=module.error.module_versions
            ) if module.error else None
        ) for module in normal_data_package.modules
    ]

    return grpc_data_package_class(
        id=normal_data_package.id,
        pipeline_id=normal_data_package.pipeline_id,
        pipeline_executer_id=normal_data_package.pipeline_executer_id,
        sequence_number=normal_data_package.sequence_number,
        modules=modules,
        data=pickle.dumps(normal_data_package.data),
        running=normal_data_package.running,
        success=normal_data_package.success,
        message=normal_data_package.message,
        error=grpc_error
    )

# Example usage code
def worker(data_package):
    try:
        # Simulate work
        time.sleep(5)
        # Modify the data package
        data_package.data = {"updated_key": "updated_value"}
        print("DataPackage modified by worker thread.")
    except RuntimeError as e:
        print(e)

def main():
    data_package = DataPackage(
        pipeline_id="pipeline_1",
        pipeline_executer_id="executor_1",
        sequence_number=1,
        data=b'\x00\x01'  # Non-serializable data (bytes)
    )

    print(data_package)
    
    # Create an instance of DataPackage
    package = DataPackage(
        pipeline_id="pipeline_1",
        pipeline_executer_id="executor_1",
        sequence_number=1,
        data={"key": "value"},
        success=True,
        error="No error"
    )

    # Start the worker thread
    worker_thread = threading.Thread(target=worker, args=(package,))
    worker_thread.timed_out = False
    worker_thread.start()
    worker_thread.join(timeout=1)  # Set timeout for the worker thread

    # Check if the worker thread is still alive
    if worker_thread.is_alive():
        print("Worker thread timed out.")
        worker_thread.timed_out = True

    # Access properties
    print(f"ID: {package.id}")
    print(f"Pipeline Executor ID: {package.pipeline_executer_id}")
    print(f"Sequence Number: {package.sequence_number}")
    print(f"Data: {package.data}")
    print(f"Success: {package.success}")
    print(f"Error: {package.error}")

    # Attempt to modify the package after timeout
    try:
        package.sequence_number = 2
    except RuntimeError as e:
        print(f"Error in main thread: {e}")


    dpm = DataPackageModule(
        module_id="module_1",
        start_time=0.0,
        end_time=5.0,
        waiting_time=0.0,
        processing_time=5.0,
        total_time=5.0,
        success=True,
        error=ValueError("An example error")
    )

    print(dpm.error)

if __name__ == "__main__":
    main()
