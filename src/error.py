import json
import os
import sys
import threading
import traceback
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Dict, List, NamedTuple, Optional, Union

@dataclass
class Error:
    type: str
    message: str
    traceback: List[str]
    thread: Optional[str] = None
    start_context: Optional[str] = None
    thread_id: Optional[int] = None
    is_daemon: Optional[bool] = None
    local_vars: Optional[Dict[str, str]] = field(default_factory=dict)
    global_vars: Optional[Dict[str, str]] = field(default_factory=dict)
    environment_vars: Optional[Dict[str, str]] = field(default_factory=dict)
    module_versions: Optional[Dict[str, str]] = field(default_factory=dict)

    def __str__(self) -> str:
        return json_error_handler_str(self)

class ErrorLoggerOptions(NamedTuple):
    exc_type: bool = True
    message: bool = True
    traceback: bool = True
    thread: bool = True
    start_context: bool = True
    thread_id: bool = True
    is_daemon: bool = True
    local_vars: bool = False
    global_vars: bool = False
    environment_vars: bool = False
    module_versions: bool = False

class ErrorLogger:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ErrorLogger, cls).__new__(cls)
                    cls._instance.options = ErrorLoggerOptions()
                    cls._instance.debug = True
        return cls._instance

    def set_options(self, options: ErrorLoggerOptions) -> None:
        with self._lock:
            self.options = options

    def get_options(self) -> ErrorLoggerOptions:
        with self._lock:
            return self.options
    
    def set_debug(self, debug: bool) -> None:
        with self._lock:
            self.debug = debug
    
    def get_debug(self) -> bool:
        with self._lock:
            return self.debug

def format_vars(variables: Dict[str, Any]) -> Dict[str, str]:
    formatted_vars = {}
    for key, value in variables.items():
        formatted_vars[key] = f"{type(value).__name__} = {repr(value)}"
    return formatted_vars

def exception_to_error(exc: BaseException) -> Error:
    exc_type = type(exc)
    exc_value = exc
    exc_traceback = exc.__traceback__
    
    tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
    formatted_traceback: List[str] = []

    local_vars: Dict[str, str] = {}
    global_vars: Dict[str, str] = {}

    if exc_traceback is not None:
        tb = exc_traceback
        while tb.tb_next:
            tb = tb.tb_next
        frame = tb.tb_frame
        local_vars = format_vars(frame.f_locals)
        global_vars = format_vars({key: value for key, value in frame.f_globals.items() if not key.startswith('__')})

    for line in tb_lines:
        if line.startswith('  File '):
            parts = line.strip().split(',')
            file_path = parts[0].split('"')[1]
            line_number = parts[1].split(' ')[-1]
            formatted_traceback.append(f"{file_path}:{line_number}")
        else:
            formatted_traceback.append(line.strip())

    current_thread = threading.current_thread()
    start_context = getattr(current_thread, 'start_context', 'N/A')

    error = Error(
        type=exc_type.__name__,
        message=str(exc_value),
        traceback=formatted_traceback,
        thread=current_thread.name,
        start_context=start_context,
        thread_id=current_thread.ident,
        is_daemon=current_thread.daemon,
        local_vars=local_vars,
        global_vars=global_vars,
        environment_vars={key: os.environ[key] for key in os.environ},
        module_versions={module: sys.modules[module].__version__ if hasattr(sys.modules[module], '__version__') else 'N/A' for module in sys.modules},
    )

    return error

def json_error_handler_dict(exc: Union[BaseException, Error]) -> Dict[str, str]:
    if isinstance(exc, BaseException):
        error_obj = exception_to_error(exc)
    else:
        error_obj = exc

    error_logger = ErrorLogger()
    options = error_logger.get_options()
    
    minimal_error_info = {
        "message": "Something went wrong.",
    }

    if error_logger.get_debug():
        if options.exc_type:
            minimal_error_info["type"] = error_obj.type
        if options.message:
            minimal_error_info["message"] = error_obj.message
        if options.traceback:
            minimal_error_info["traceback"] = error_obj.traceback
        if options.thread:
            minimal_error_info["thread"] = error_obj.thread
        if options.start_context:
            minimal_error_info["start_context"] = error_obj.start_context
        if options.thread_id:
            minimal_error_info["thread_id"] = error_obj.thread_id
        if options.is_daemon:
            minimal_error_info["is_daemon"] = error_obj.is_daemon
        if options.local_vars:
            minimal_error_info["local_vars"] = error_obj.local_vars
        if options.global_vars:
            minimal_error_info["global_vars"] = error_obj.global_vars
        if options.environment_vars:
            minimal_error_info["environment_vars"] = error_obj.environment_vars
        if options.module_versions:
            minimal_error_info["module_versions"] = error_obj.module_versions

    return minimal_error_info

def json_error_handler_str(exc: Union[BaseException, Error]) -> str:
    minimal_error_info = json_error_handler_dict(exc) 
    error_json = json.dumps(minimal_error_info, indent=4)
    return error_json

# Custom JSON error handler
def json_error_handler(exc: Union[BaseException, Error]) -> None:
    error_json = json_error_handler_str(exc)
    print(error_json)

# Set the custom error handler for uncaught exceptions
def custom_excepthook(exc_type: type[BaseException], exc_value: BaseException, exc_traceback: TracebackType | None) -> None:
    json_error_handler(exc_value)

sys.excepthook = custom_excepthook

# Set the custom error handler for uncaught exceptions in threads
def threading_excepthook(args: threading.ExceptHookArgs) -> None:
    json_error_handler(args.exc_value)

threading.excepthook = threading_excepthook

# Example usage
if __name__ == "__main__":
    error_logger = ErrorLogger()
    error_logger.set_debug(True)  # Enable or disable debug logging

    # Set custom options
    custom_options = ErrorLoggerOptions(
        exc_type = True,
        message = True,
        traceback = True,
        thread = True,
        start_context = True,
        thread_id = True,
        is_daemon = False,
        local_vars = True,
        global_vars = False,
        environment_vars = False,
        module_versions = False,
    )
    error_logger.set_options(custom_options)

    def faulty_function() -> None:
        x = 42
        y = 'example'
        raise ValueError("An example error for testing")
    
    def execute_function() -> None:
        faulty_function()

    def thread_level_3() -> None:
        execute_function()

    def thread_level_2() -> None:
        thread3 = threading.Thread(target=thread_level_3, name="Thread-Level-3")
        thread3.start_context = threading.current_thread().name
        thread3.start()
        thread3.join()

    def thread_level_1() -> None:
        thread2 = threading.Thread(target=thread_level_2, name="Thread-Level-2")
        thread2.start_context = threading.current_thread().name
        thread2.start()
        thread2.join()

    # Start the top-level thread
    print(f"-------------------In Thread-------------------")
    thread1 = threading.Thread(target=thread_level_1, name="Thread-Level-1")
    thread1.start_context = threading.current_thread().name
    thread1.start()
    thread1.join()
    print("-----------------------------------------------\n")

    # Example usage in the main thread to also demonstrate main thread error handling
    print(f"---------In MainThread with try except---------")
    try:
        execute_function()
    except Exception as e:
        json_error_handler(e)
    print(f"-----------------------------------------------\n")
        
    print(f"-----------In MainThread with crash------------")
    execute_function()
    print(f"-----------------------------------------------\n")
