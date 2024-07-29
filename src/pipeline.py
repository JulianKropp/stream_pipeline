from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
import threading
from typing import Any, Callable, Dict, List, Tuple, Union
import unittest
from unittest.mock import MagicMock
import uuid
from prometheus_client import Gauge, Summary
from .data_package import DataPackage

from .module_classes import Module


PIPELINE_PROCESSING_COUNTER = Gauge('pipeline_processing_counter', 'Number of processes executing the pipline at the moment', ['pipeline_name'])


class PipelineProcessingPhase:
    """
    Class to manage and execute a sequence of modules.
    Args:
        modules (List[Module]): List of modules to execute.
        name (str): Name of the processing phase. (Default: PPP-{id})
    """
    def __init__(self, modules: List[Module], name: str = "") -> None:
        self._id = f"PPP-{uuid.uuid4()}"
        self._name = name if name else self._id
        self._modules = modules.copy()

    def get_id(self) -> str:
        """
        Returns the unique identifier of the processing phase.
        """
        return self._id

    def get_name(self) -> str:
        """
        Returns the name of the processing phase.
        """
        return self._name

    def run(self, data_package: DataPackage):
        """
        Executes the pipeline processing phase with the given data package.
        Args:
            data_package (DataPackage): The data package to process.
        """
        for module in self._modules:
            module.run(data_package)
            if not data_package.success:
                return


class PipelineExecutor:
    """
    Class to manage the execution of a pipeline by creating DataPackages and executing the PipelineProcessingPhases. Also keeps track of the order of the data packages.
    Args:
        name (str): Name of the pipeline executor. (Default: PE-{id})
    """
    
    def __init__(self, pipline_id: str, name: str = "") -> None:
        self._id = f"PE-{uuid.uuid4()}"
        self._pipline_id = pipline_id
        self._name = name if name else self._id
        self._next_sequence_number = 0
        self._last_finished_sequence_number = -1

        self._data_packages: Dict[int, DataPackage] = {}
        self._finished_data_packages: Dict[int, DataPackage] = {}

        self._lock = threading.Lock()

    def get_id(self) -> str:
        """
        Returns the unique identifier of the pipeline executor.
        """
        return self._id

    def get_name(self) -> str:
        """
        Returns the name of the pipeline executor.
        """
        return self._name

    def get_last_finished_sequence_number(self) -> int:
        """
        Returns the last finished sequence number.
        """
        return self._last_finished_sequence_number

    def set_last_finished_sequence_number(self, sequence_number: int) -> None:
        """
        Sets the last finished sequence number.
        Args:
            sequence_number (int): The sequence number to set as the last finished.
        """
        if sequence_number >= self._next_sequence_number or sequence_number < self._last_finished_sequence_number:
            raise ValueError("Sequence number cannot be greater or equal than the next sequence number or smaller than the last finished sequence number.")
        self._last_finished_sequence_number = sequence_number

    def run(self, pipeline_processing_phases: List[PipelineProcessingPhase], sequence_number: int) -> None:
        """
        Executes the pipeline process with the given data package.
        Args:
            pipeline_processing_phases (List[PipelineProcessingPhase]): List of processing phases to execute.
            sequence_number (int): The sequence number of the data package to process.
        """
        with self._lock:
            data_package = self._data_packages.get(sequence_number)
            if not data_package:
                raise ValueError(f"Data package with sequence number {sequence_number} not found. First add data with add_data(data: Any)")

        for phase in pipeline_processing_phases:
            phase.run(data_package)
            if not data_package.success:
                return

    def add_data(self, data: Union[DataPackage, Any]) -> DataPackage:
        """
        Adds a new data package to the executor.
        Args:
            data (Union[DataPackage, Any]): The data to add to the executor. Can be an instance of DataPackage from gRPC or any other data.
        Returns:
            DataPackage: The newly created data package.
        """
        if isinstance(data, DataPackage):
            data_package = data
        else:
            data_package = DataPackage(
                pipeline_id=self._pipline_id,
                pipeline_executer_id=self._id,
                sequence_number=self._next_sequence_number,
                data=data
            )

        with self._lock:
            self._data_packages[self._next_sequence_number] = data_package
            self._next_sequence_number += 1
            return data_package

    def remove_data(self, sequence_number: int) -> None:
        """
        Removes a data package based on its sequence number.
        Args:
            sequence_number (int): The sequence number of the data package to remove.
        """
        with self._lock:
            self._data_packages.pop(sequence_number, None)
            self._finished_data_packages.pop(sequence_number, None)

    def push_finished_data_package(self, sequence_number: int) -> None:
        """
        Pushes a data package to the finished queue based on its sequence number.
        Args:
            sequence_number (int): The sequence number of the data package to push to the finished queue.
        """
        with self._lock:
            if sequence_number in self._data_packages:
                data_package = self._data_packages.pop(sequence_number)
                self._finished_data_packages[sequence_number] = data_package

    def pop_finished_data_packages(self) -> Dict[str, DataPackage]:
        """
        Returns a dict of finished data packages in order of sequence number until the first missing sequence number and removes them from the queue.
        Example:
            Queue: [1, 2, 5, 6, 7]
                -> Returns: [1, 2]
            Queue left: [5, 6, 7]
        Returns:
            Dict[str, DataPackage]: Dictionary of finished data packages.
        """
        with self._lock:
            finished_data_packages: Dict[str, DataPackage] = {}
            current_sequence = self._last_finished_sequence_number + 1

            while current_sequence in self._finished_data_packages:
                data_package = self._finished_data_packages.pop(current_sequence)
                finished_data_packages[data_package.id] = data_package
                self._last_finished_sequence_number = current_sequence
                current_sequence += 1

            return finished_data_packages


class PipelineMode(Enum):
    """
    Enum to define different modes of pipeline execution.
    Values:
        ORDER_BY_SEQUENCE: Orders at the end all DataPackages by how they were added at the beginning of the pipeline.
        FIRST_WINS: First DataPackage that finishes is the one that is returned. All others are discarded.
        NO_ORDER: No specific order for the DataPackages. They are returned as they finish.
    """
    ORDER_BY_SEQUENCE = 1
    FIRST_WINS = 2
    NO_ORDER = 3


class Pipeline:
    """
    Class to manage pre-processing, main processing, and post-processing stages.
    Args:
        pre_modules (List[Module]): List of pre-processing modules.
        main_modules (List[Module]): List of main processing modules.
        post_modules (List[Module]): List of post-processing modules.
        name (str): Name of the pipeline. (Default: P-{id})
        max_workers (int): Maximum number of worker threads. If 0, no threads are used. That means the pipeline and the callback function will run in the main thread. This can block the main thread. (Default: 10)
        mode (PipelineMode): The mode of pipeline execution. (ORDER_BY_SEQUENCE, FIRST_WINS, NO_ORDER) (Default: PipelineMode.ORDER_BY_SEQUENCE)
    """
    def __init__(self, pre_modules: List[Module], main_modules: List[Module], post_modules: List[Module], name: str = "", max_workers: int = 10, mode: PipelineMode = PipelineMode.ORDER_BY_SEQUENCE) -> None:
        self._id: str = f"P-{uuid.uuid4()}"
        self._name: str = name if name else self._id
        self._pre_modules = PipelineProcessingPhase(pre_modules, name=f"PPP-{self._name}-pre")
        self._main_modules = PipelineProcessingPhase(main_modules, name=f"PPP-{self._name}-main")
        self._post_modules = PipelineProcessingPhase(post_modules, name=f"PPP-{self._name}-post")
        self._max_workers: int = max_workers
        self._mode: PipelineMode = mode
        self._executor_map: Dict[int, PipelineExecutor] = {}
        self.active_futures: Dict[str, Future] = {}

        self._lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers) if max_workers > 0 else None

    def get_id(self) -> str:
        """
        Returns the unique identifier of the pipeline.
        """
        return self._id

    def get_name(self) -> str:
        """
        Returns the name of the pipeline.
        """
        return self._name

    def set_pre_modules(self, modules: List[Module]) -> None:
        """
        Sets the pre-processing modules.
        Args:
            modules (List[Module]): List of pre-processing modules. This will apply for all new requests to the pipeline.
        """
        with self._lock:
            self._pre_modules = PipelineProcessingPhase(modules, name=f"PPP-{self._name}-pre")

    def set_main_modules(self, modules: List[Module]) -> None:
        """
        Sets the main processing modules.
        Args:
            modules (List[Module]): List of main processing modules. This will apply for all new requests to the pipeline.
        """
        with self._lock:
            self._main_modules = PipelineProcessingPhase(modules, name=f"PPP-{self._name}-main")

    def set_post_modules(self, modules: List[Module]) -> None:
        """
        Sets the post-processing modules. This will apply for all new requests to the pipeline.
        Args:
            modules (List[Module]): List of post-processing modules.
        """
        with self._lock:
            self._post_modules = PipelineProcessingPhase(modules, name=f"PPP-{self._name}-post")

    def set_max_workers(self, max_workers: int) -> None:
        """
        Sets the maximum number of worker threads.
        Args:
            max_workers (int): Maximum number of worker threads. Will apply immediately to all requests including the ones that are currently in the queue.
        """
        with self._lock:
            self._max_workers = max_workers
            self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def set_mode(self, mode: PipelineMode) -> None:
        """
        Sets the mode of pipeline execution.
        Args:
            mode (PipelineMode): The execution mode to set. Will apply immediately to all requests including the ones that are currently in the queue and running.
        """
        with self._lock:
            self._mode = mode

    def run(self, data: Any, callback: Callable[[DataPackage], None], error_callback: Union[Callable[[DataPackage], None], None] = None) -> None:
        """
        Executes the pipeline with the given data.
        Args:
            data (Any): The data to process.
            callback (Callable[[str, DataPackage], None]): The callback function to call with the result.
            error_callback (Callable[[str, DataPackage], None]): The callback function to call in case of an error. (Default: None)
        """
        PIPELINE_PROCESSING_COUNTER.labels(pipeline_name=self._name).inc()
        
        callback_id = id(callback)
        with self._lock:
            executor = self._executor_map.get(callback_id)
            if executor is None:
                executor = PipelineExecutor(pipline_id=self._id, name=f"PE-{self._name}-process-{callback_id}")
                self._executor_map[callback_id] = executor

        data_package = executor.add_data(data)

        start_context = threading.current_thread().name

        def execute_pipeline() -> None:
            try:
                threading.current_thread().start_context = start_context # type: ignore
                
                with self._lock:
                    pipeline_processing_phases = [self._pre_modules, self._main_modules, self._post_modules]

                executor.run(pipeline_processing_phases, data_package.sequence_number)

                if self._mode == PipelineMode.ORDER_BY_SEQUENCE:
                    executor.push_finished_data_package(data_package.sequence_number)
                    finished_data_packages = executor.pop_finished_data_packages()
                    for _, finished_data_package in finished_data_packages.items():
                        if finished_data_package.success:
                            callback(finished_data_package)
                        else:
                            if error_callback:
                                error_callback(finished_data_package)

                elif self._mode == PipelineMode.FIRST_WINS:
                    with self._lock:
                        last_finished_sequence_number = executor.get_last_finished_sequence_number()
                        if data_package.sequence_number <= last_finished_sequence_number and data_package.success:
                            self.active_futures.pop(f"{executor.get_id()}-{data_package.sequence_number}")
                            executor.remove_data(data_package.sequence_number)
                            if not data_package.success:
                                error_callback(data_package.message, data_package.data)
                        else:
                            executor.set_last_finished_sequence_number(data_package.sequence_number)
                            callback(data_package.data)

                elif self._mode == PipelineMode.NO_ORDER:
                    executor.remove_data(data_package.sequence_number)
                    if data_package.success:
                        callback(data_package)
                    else:
                        if error_callback:
                            error_callback(data_package.data)
            
            except Exception as e:
                print("CRITICAL ERROR")
                executor.remove_data(data_package.sequence_number)
                data_package.success = False
                data_package.errors.append(e)
                if error_callback:
                    error_callback(data_package)
                
            PIPELINE_PROCESSING_COUNTER.labels(pipeline_name=self._name).dec()

        if self.executor is None:
            execute_pipeline()
            return

        eid = f"{executor.get_id()}-{data_package.sequence_number}"
        future = self.executor.submit(execute_pipeline)
        if self._mode == PipelineMode.FIRST_WINS:
            self.active_futures[eid] = future
        print(f"Task {eid} submitted")





class TestPipeline(unittest.TestCase):
    
    class MockModule(Module):
        def execute(self, data):
            return True, "Success", data
    
    class MockFailingModule(Module):
        def execute(self, data):
            return False, "Failure", data

    class MockExceptionModule(Module):
        def execute(self, data):
            raise Exception("Exception")

    def setUp(self):
        self.data_package = DataPackage(
            id="DP-123",
            pipeline_executer_id="PIPE-123",
            sequence_number=0,
            data="test_data"
        )
        self.mock_module = self.MockModule()
        self.mock_failing_module = self.MockFailingModule()
        self.mock_exception_module = self.MockExceptionModule()

    def test_pipeline_processing_phase_success(self):
        phase = PipelineProcessingPhase(modules=[self.mock_module], name="TestPhase")
        success, message, result = phase.run(self.data_package)
        self.assertTrue(success)
        self.assertEqual(message, "Modules MockModule succeeded")
        self.assertEqual(result.data, "test_data")

    def test_pipeline_processing_phase_failure(self):
        phase = PipelineProcessingPhase(modules=[self.mock_failing_module], name="TestPhase")
        success, message, result = phase.run(self.data_package)
        self.assertFalse(success)
        self.assertEqual(message, "Module MockFailingModule failed: Failure")

    def test_pipeline_processing_phase_exception(self):
        phase = PipelineProcessingPhase(modules=[self.mock_exception_module], name="TestPhase")
        success, message, result = phase.run(self.data_package)
        self.assertFalse(success)
        self.assertIn("Module MockExceptionModule failed with error", message)

    def test_pipeline_executor_add_data(self):
        executor = PipelineExecutor(pipline_id="pipline_id", name="TestExecutor")
        data_package = executor.add_data("test_data")
        self.assertEqual(data_package.data, "test_data")
        self.assertEqual(data_package.sequence_number, 0)

    def test_pipeline_executor_run_success(self):
        executor = PipelineExecutor(pipline_id="pipline_id", name="TestExecutor")
        data_package = executor.add_data("test_data")
        phase = PipelineProcessingPhase(modules=[self.mock_module], name="TestPhase")
        success, message, result = executor.run([phase], data_package.sequence_number)
        self.assertTrue(success)
        self.assertEqual(message, "All pipeline phases succeeded")

    def test_pipeline_executor_run_failure(self):
        executor = PipelineExecutor(pipline_id="pipline_id", name="TestExecutor")
        data_package = executor.add_data("test_data")
        phase = PipelineProcessingPhase(modules=[self.mock_failing_module], name="TestPhase")
        success, message, result = executor.run([phase], data_package.sequence_number)
        self.assertFalse(success)
        self.assertIn("Pipeline TestExecutor failed", message)

    def test_pipeline_executor_run_exception(self):
        executor = PipelineExecutor(pipline_id="pipline_id", name="TestExecutor")
        data_package = executor.add_data("test_data")
        phase = PipelineProcessingPhase(modules=[self.mock_exception_module], name="TestPhase")
        success, message, result = executor.run([phase], data_package.sequence_number)
        self.assertFalse(success)
        self.assertIn("Pipeline TestExecutor failed", message)

    def test_pipeline_run_order_by_sequence(self):
        pipeline = Pipeline(
            pre_modules=[self.mock_module],
            main_modules=[self.mock_module],
            post_modules=[self.mock_module],
            name="TestPipeline",
            mode=PipelineMode.ORDER_BY_SEQUENCE
        )
        callback = MagicMock()
        pipeline.run("test_data", callback)
        callback.assert_called()

    def test_pipeline_run_first_wins(self):
        pipeline = Pipeline(
            pre_modules=[self.mock_module],
            main_modules=[self.mock_module],
            post_modules=[self.mock_module],
            name="TestPipeline",
            mode=PipelineMode.FIRST_WINS
        )
        callback = MagicMock()
        pipeline.run("test_data", callback)
        callback.assert_called()

    def test_pipeline_run_no_order(self):
        pipeline = Pipeline(
            pre_modules=[self.mock_module],
            main_modules=[self.mock_module],
            post_modules=[self.mock_module],
            name="TestPipeline",
            mode=PipelineMode.NO_ORDER
        )
        callback = MagicMock()
        pipeline.run("test_data", callback)
        callback.assert_called()


if __name__ == '__main__':
    unittest.main()