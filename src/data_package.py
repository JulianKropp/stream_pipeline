from dataclasses import dataclass, field, asdict
import json
import types
from typing import Any, Dict, List, Optional, Tuple, Union
import threading
import uuid
import copy
import time

from .error import exception_to_error, json_error_handler_dict, Error

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
    error: Optional[Union[Exception, Error]] = None # This will only be needed if init an obj with error
    _error: Optional[Error] = field(default=None, init=False)

    def __post_init__(self): # This will set error of the init obj to _error
        self.error = self.error  # This will trigger the setter
        # Remove self.error from the __dict__ to avoid recursion
        if 'error' in self.__dict__:
            del self.__dict__['error']

    @property
    def error(self) -> Optional[Error]:
        return self._error

    @error.setter
    def error(self, value: Union[Exception, Error, None]) -> None:
        if isinstance(value, Exception):
            self._error = exception_to_error(value)
        elif isinstance(value, Error):
            self._error = value
        else:
            self._error = None



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
    id: str = field(default_factory=lambda: "DP-" + str(uuid.uuid4()), init=False)
    pipeline_id: str
    pipeline_executer_id: str
    sequence_number: int
    modules: List[Any] = field(default_factory=list)  # Replace Any with DataPackageModule if defined
    data: Any = None
    running: bool = False
    success: bool = True
    message: str = ""
    error: Optional[Union[Exception, Error]] = None # This will only be needed if init an obj with error
    _error: Optional[Error] = field(default=None, init=False)

    def __post_init__(self): # This will set error of the init obj to _error
        self.error = self.error  # This will trigger the setter
        # Remove self.error from the __dict__ to avoid recursion
        if 'error' in self.__dict__:
            del self.__dict__['error']

    @property
    def error(self) -> Optional[Error]:
        return self._error

    @error.setter
    def error(self, value: Union[Exception, Error, None]) -> None:
        if isinstance(value, Exception):
            self._error = exception_to_error(value)
        elif isinstance(value, Error):
            self._error = value
        else:
            self._error = None

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
        data_dict['modules'] = [asdict(module) for module in data_dict['modules']]

        # Handle non-serializable data
        try:
            jsonstring = json.dumps(data_dict, default=str, indent=4)
        except (TypeError, ValueError):
            data_dict['data'] = "Data which cannot be displayed as JSON"
            jsonstring = json.dumps(data_dict, default=str, indent=4)

        return jsonstring


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

    dpm.error = ValueError("Another example error")

    print(dpm.error)

if __name__ == "__main__":
    main()
