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
class ThreadSafeClass:
    _mutex: threading.Lock = threading.Lock()  # For locking all properties that start with '_'
    _mutexes: Dict[str, threading.Lock] = field(default_factory=dict, init=False)
    _immutable_attributes: List[str] = field(default_factory=list)

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
        if '_immutable_attributes' in self.__dict__:
            for attr in self._immutable_attributes:
                if name == attr and attr in self.__dict__:
                    raise AttributeError(f"'{self.__class__.__name__}' object attribute '{attr}' is immutable")

        if name.startswith('_'):
            with self._mutex:
                super().__setattr__(name, value)
                return

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
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        for k, v in self.__dict__.items():
            if k != '_mutexes':
                setattr(result, k, copy.deepcopy(v, memo))

        result._mutexes = {k: threading.Lock() for k in self.__dict__.keys() if k != '_mutexes'}
        return result

    def copy(self):
        return copy.deepcopy(self)
    
    def to_dict(self):
        def process_dict(data):
            if isinstance(data, dict):
                return {k: process_dict(v) for k, v in data.items()}
            elif isinstance(data, (list, tuple, set)):
                return type(data)(process_dict(item) for item in data)
            elif isinstance(data, BaseException):
                return json_error_handler_dict(data)
            elif hasattr(data, 'to_dict'):
                return data.to_dict()
            else:
                return data
        
        result = {}
        for key, value in self.__dict__.items():
            if key.startswith('_'):
                continue
            result[key] = process_dict(value)
        return result

    def __str__(self):
        data_dict = self.to_dict()
        # Handle non-serializable data
        try:
            jsonstring = json.dumps(data_dict, default=str, indent=4)
        except (TypeError, ValueError):
            data_dict['data'] = "Data which cannot be displayed as JSON"
            jsonstring = json.dumps(data_dict, default=str, indent=4)

        return jsonstring




@dataclass
class DataPackageModule(ThreadSafeClass):
    """
    Class which contains metadata for a module that has processed a data package.
    Attributes:
        module_id (str):                        ID of the module.
        running (bool):                         Indicates if the module is running.
        start_time (float):                     Time when the module was started.
        end_time (float):                       Time when the module finished.
        waiting_time (float):                   Time spent waiting for the mutex to unlock.
        processing_time (float):                Time spent processing the data package.
        total_time (float):                     Total time spent processing the data package.
        success (bool):                         Indicates if the process was successful.
        error (Exception or Error):             Contains the error. Can be set as type Exception or Error and will be converted to Error.
        sub_modules (List[DataPackageModule]):  List of sub-modules that have processed the data package. Including measurements.
    """
    module_id: str = ""
    running: bool = False
    start_time: float = 0.0
    end_time: float = 0.0
    waiting_time: float = 0.0
    processing_time: float = 0.0
    total_time: float = 0.0
    success: bool = True
    error: Union[Exception, Error, None] = None
    sub_modules: List['DataPackageModule'] = field(default_factory=list)



@dataclass
class DataPackage(ThreadSafeClass):
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
        errors Union[Error, Exception]]:        Error that occurred during the processing of the data package.
    """
    pipeline_id: str = ""
    pipeline_executer_id: str = ""
    sequence_number: int = -1
    id: str = ""
    modules: List['DataPackageModule'] = field(default_factory=list)
    data: Any = None
    running: bool = False
    success: bool = True
    message: str = ""
    error: Union[Error, Exception, None] = None

    def __post_init__(self):
        self._immutable_attributes = ['id', 'pipeline_id', 'pipeline_executer_id']


# def grpc_to_normal(grpc_data_package: Any, data_package: Union[None, DataPackage] = None) -> DataPackage:


# def normal_to_grpc(normal_data_package: DataPackage):
    # pass