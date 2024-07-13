# main.py
import random
import threading
from src.module_classes import ExecutionModule, ConditionModule, CombinationModule, ModuleOptions
from src.pipeline import Pipeline, PipelineMode
from prometheus_client import start_http_server
import concurrent.futures
import time
from src.data_package import DataPackage

# Start up the server to expose the metrics.
start_http_server(8000)

# Example custom modules
class DataValidationModule(ExecutionModule):
    def execute(self, data_package) -> DataPackage:
        data = data_package.data
        if isinstance(data, dict) and "key" in data:
            data_package.success = True
            data_package.message = "Validation succeeded"
            data_package.data = data
        else:
            data_package.success = False
            data_package.message = "Validation failed: key missing"
            data_package.data = data

        return data_package

class DataTransformationModule(ExecutionModule):
    def __init__(self):
        super().__init__(ModuleOptions(
            use_mutex=False,
            timeout=20.0
        ))

    def execute(self, data_package: DataPackage) -> DataPackage:
        data = data_package.data
        list1 = [1, 2, 3, 4, 5, 6]
        randomint = random.choice(list1)
        time.sleep(randomint)
        if "key" in data:
            data["key"] = data["key"].upper()
            data_package.success = True
            data_package.message = "Transformation succeeded"
            data_package.data = data
        else:
            data_package.success = False
            data_package.message = "Transformation failed: key missing"
            data_package.data = data

        return data_package

class DataConditionModule(ConditionModule):
    def condition(self, data_package: DataPackage) -> bool:
        data = data_package.data
        return "condition" in data and data["condition"] == True

class SuccessModule(ExecutionModule):
    def execute(self, data_package: DataPackage) -> DataPackage:
        data = data_package.data
        data["status"] = "success"
        data_package.success = True
        data_package.message = "Condition true: success"
        data_package.data = data
        return data_package

class FailureModule(ExecutionModule):
    def execute(self, data_package: DataPackage) -> DataPackage:
        data = data_package.data
        data["status"] = "failure"
        data_package.success = True
        data_package.message = "Condition false: failure"
        data_package.data = data
        return data_package

class AlwaysTrue(ExecutionModule):
    def execute(self, data_package: DataPackage) -> DataPackage:
        data_package.success = True
        data_package.message = "Always true"
        return data_package

# Setting up the processing pipeline
pre_modules = [DataValidationModule()]
main_modules = [
    DataTransformationModule(),
    DataConditionModule(SuccessModule(), FailureModule())
]
post_modules = [
    CombinationModule([
        CombinationModule([
            AlwaysTrue(),
        ]),
    ])
]


manager = Pipeline(pre_modules, main_modules, post_modules, "test-pipeline", 10, PipelineMode.ORDER_BY_SEQUENCE)

counter = 0
counter_mutex = threading.Lock()
def callback(message, processed_data):
    global counter, counter_mutex
    print(message, processed_data)
    with counter_mutex:
        counter = counter + 1

def error_callback(message, processed_data):
    global counter, counter_mutex
    print(f"ERROR: {message}, data: {processed_data}")
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