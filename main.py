# main.py
import random
import threading
from src.module_classes import ExecutionModule, ConditionModule, CombinationModule, Module, ModuleOptions, DataPackage
from src.pipeline import Pipeline, PipelineMode
from prometheus_client import start_http_server
import concurrent.futures
import time
import src.error as error

err_logger = error.ErrorLogger()
err_logger.set_debug(True)


# Start up the server to expose the metrics.
start_http_server(8000)

# Example custom modules
class DataValidationModule(ExecutionModule):
    def execute(self, data: DataPackage) -> None:
        if isinstance(data.data, dict) and "key" in data.data:
            data.success = True
            data.message = "Validation succeeded"
        else:
            data.success = False
            data.message = "Validation failed: key missing"

class DataTransformationModule(ExecutionModule):
    def __init__(self):
        super().__init__(ModuleOptions(
            use_mutex=False,
            timeout=4.0
        ))

    def execute(self, data: DataPackage) -> None:
        list1 = [1, 2, 3, 4, 5, 6]
        randomint = random.choice(list1)
        time.sleep(randomint)
        if "key" in data.data:
            data.data["key"] = data.data["key"].upper()
            data.success = True
            data.message = "Transformation succeeded"
        else:
            data.success = False
            data.message = "Transformation failed: key missing"

class DataConditionModule(ConditionModule):
    def condition(self, data: DataPackage) -> bool:
        return "condition" in data.data and data.data["condition"] == True

class SuccessModule(ExecutionModule):
    def execute(self, data: DataPackage) -> None:
        data.data["status"] = "success"
        data.success = True
        data.message = "Condition true: success"

class FailureModule(ExecutionModule):
    def execute(self, data: DataPackage) -> None:
        data.data["status"] = "failure"
        data.success = True
        data.message = "Condition false: failure"

class AlwaysTrue(ExecutionModule):
    def execute(self, data: DataPackage) -> None:
        data.success = True
        data.message = "Always true"

# Setting up the processing pipeline
pre_modules: list[Module] = [
    DataValidationModule()]
main_modules: list[Module] = [
    DataTransformationModule(),
    DataConditionModule(SuccessModule(), FailureModule()),
]
post_modules: list[Module] = [
    CombinationModule([
        CombinationModule([
            AlwaysTrue(),
        ]),
    ])
]


manager = Pipeline(pre_modules, main_modules, post_modules, "test-pipeline", 10, PipelineMode.ORDER_BY_SEQUENCE)

counter = 0
counter_mutex = threading.Lock()
def callback(processed_data: DataPackage):
    global counter, counter_mutex
    print(f"OK: {processed_data.message}")
    with counter_mutex:
        counter = counter + 1

def error_callback(processed_data: DataPackage):
    global counter, counter_mutex
    # print(f"ERROR: {processed_data}, data: {processed_data.data}: {processed_data.errors}")
    print(f"ERROR: {processed_data.message}")
    with counter_mutex:
        counter = counter + 1

# Function to execute the processing pipeline
def process_data(data):
    manager.run(data, callback, error_callback)

# Example data
data_list = [
    {"key": "value0", "condition": True},
    {"key": "value1", "condition": False},
    {"key": "value2", "condition": True},
    {"key": "value3", "condition": False},
    {"key": "value4", "condition": True},
    {"key": "value5", "condition": False},
    {"key": "value6", "condition": True},
    {"key": "value7", "condition": False},
    {"key": "value8", "condition": True},
    {"key": "value9", "condition": False},
]

for d in data_list:
    process_data(d)

# # Using ThreadPoolExecutor for multithreading
# with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
#     futures = [executor.submit(process_data, data) for data in data_list]

# Keep the main thread alive
while True:
    time.sleep(1)
    if counter >= len(data_list):
        break
