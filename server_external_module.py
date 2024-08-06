def main() -> None:
    import random
    import time
    from typing import Union
    from stream_pipeline.grpc_server import GrpcServer
    from stream_pipeline.data_package import DataPackage, DataPackageModule
    from stream_pipeline.module_classes import ExecutionModule, ModuleOptions


    class TestModule(ExecutionModule):
        def __init__(self) -> None:
            super().__init__(ModuleOptions(
                use_mutex=False,
                timeout=4.0
            ))

        def execute(self, data: DataPackage, dpm: DataPackageModule) -> None:
            list1 = [1, 2, 3]
            randomint = random.choice(list1)
            time.sleep(randomint)
            if "key" in data.data:
                data.data["key"] = data.data["key"].upper() + " transformed"
                dpm.success = True
                dpm.message = "Transformation succeeded"
            else:
                dpm.success = False
                dpm.message = "Transformation failed: key missing"

    # Example usage
    module = TestModule()
    server = GrpcServer(module, 50051)
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    main()