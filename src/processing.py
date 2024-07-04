from concurrent.futures import ThreadPoolExecutor
import threading
import inspect
from typing import Callable, Dict, List, Tuple, Any
from .module_classes import Module, ExecutionModule, ConditionModule, CombinationModule

class Processing:
    """
    Class to manage and execute a sequence of modules.
    """
    def __init__(self, modules: List[Module]):
        self.modulesMutex = threading.RLock()
        self._modules = modules
        try:
            self.setModules(modules)
        except Exception as e:
            raise e

    def setModules(self, modules: List[Module]):
        """
        Sets the modules for the processing sequence during runtime. Will apply on the next run.
        """
        for module in modules:
            if not isinstance(module, Module):
                raise TypeError(f"Module {module} is not a subclass of Module")
            self._validate_execute_method(module)
        
        with self.modulesMutex:
            self._modules = modules

    def _validate_execute_method(self, module: Module) -> None:
        """
        Validates that the module has an execute method with the correct signature.
        """
        execute_method = getattr(module, 'execute', None)
        if execute_method is None:
            raise TypeError(f"Module {module.__class__.__name__} does not have an 'execute' method")

        # Check the method signature
        signature = inspect.signature(execute_method)
        parameters = list(signature.parameters.values())
        if len(parameters) != 1 or parameters[0].name != 'data':
            raise TypeError(f"'execute' method of {module.__class__.__name__} must accept exactly one parameter 'data'")

    def run(self, data: Any) -> Tuple[bool, str, Any]:
        """
        Runs the sequence of modules on the given data.
        """
        modules_copy = None
        with self.modulesMutex:
            modules_copy = self._modules[:]
        
        result_data = data
        for i, module in enumerate(modules_copy):
            module_name = module.__class__.__name__
            try:
                result = module.run(result_data)

                if not (isinstance(result, tuple) and len(result) == 3 and isinstance(result[0], bool) and isinstance(result[1], str)):
                    raise TypeError(f"Module {i} ({module_name}) returned an invalid result. Expected (bool, str, Any). Got {result}")

                result, result_message, result_data = result
                if not result:
                    return False, f"Module {i} ({module_name}) failed: {result_message}", result_data
            except Exception as e:
                return False, f"Module {i} ({module_name}) failed with error: {str(e)}", result_data
        return True, "Processing succeeded", result_data

class Process:
    def __init__(self, id: int):
        self.id: int = id
        self.sequence_number_count: int = 0
        self.finished_sequence_number_count: int = -1
        self.stored_data: Dict[int, Any] = {}
        self.lock = threading.Lock()
        
    def get_sequence_number(self) -> int:
        with self.lock:
            return self.sequence_number_count
    
    def get_finished_sequence_number(self) -> int:
        with self.lock:
            return self.finished_sequence_number_count
        
    def increase_sequence_number(self):
        with self.lock:
            self.sequence_number_count += 1
        
    def increase_finished_sequence_number(self):
        with self.lock:
            self.finished_sequence_number_count += 1
        
    def store_data(self, sequence_number: int, data: Any):
        with self.lock:
            self.stored_data[sequence_number] = data
        
    def get_next_data(self) -> Any:
        with self.lock:
            data = self.stored_data.get(self.finished_sequence_number_count + 1)
            if data is not None:
                del self.stored_data[self.finished_sequence_number_count + 1]
            return data

class ProcessingManager:
    """
    Class to manage pre-processing, main processing, and post-processing stages.
    """
    def __init__(self, pre_modules: List[Any], main_modules: List[Any], post_modules: List[Any], max_workers: int = 10):
        self.pre_processing = Processing(pre_modules)
        self.main_processing = Processing(main_modules)
        self.post_processing = Processing(post_modules)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.process_map: Dict[int, Process] = {}

    def run(self, data: Any, callback: Callable[[bool, str, Any], None]) -> None:
        """
        Executes the pre-processing, main processing, and post-processing stages sequentially.
        """
        
        # Get process from self.process_map or add it by str(id(callback)) as id
        process = self.process_map.get(id(callback))
        if process is None:
            process = Process(id(callback))
            self.process_map[id(callback)] = process
        
        
        def execute(sequence_number: int) -> None:
            pre_result, pre_message, pre_data = self.pre_processing.run(data)
            if not pre_result:
                callback(False, f"Pre-processing failed: {pre_message}", pre_data)
                return

            main_result, main_message, main_data = self.main_processing.run(pre_data)
            if not main_result:
                callback(False, f"Main processing failed: {main_message}", main_data)
                return

            post_result, post_message, post_data = self.post_processing.run(main_data)
            if not post_result:
                callback(False, f"Post-processing failed: {post_message}", post_data)
                return
            
            print(post_data["key"])

            process.store_data(sequence_number, post_data)
            while True:
                next_data = process.get_next_data()
                if next_data is None:
                    break
                callback(True, "All processing succeeded", next_data)
                process.increase_finished_sequence_number()

        sequence_number = process.get_sequence_number()
        process.increase_sequence_number()
        self.executor.submit(execute, sequence_number)
        print(f"Task {sequence_number} submitted")

    def shutdown(self):
        self.executor.shutdown(wait=False)
        print("Executor shutdown")
        