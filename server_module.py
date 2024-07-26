import grpc # type: ignore
from concurrent import futures
import time
from typing import Generator

from src.data_package import grpc_to_normal, normal_to_grpc
from src.error import exception_to_error

from src import data_pb2  # Correct import statement
from src import data_pb2_grpc  # Correct import statement

class ModuleServiceServicer(data_pb2_grpc.ModuleServiceServicer):
    def run(self, request: data_pb2.DataPackage, context: grpc.ServicerContext) -> data_pb2.DataPackage: # type: ignore
        try:
            try:
                raise ValueError("This is a test error")
            except ValueError as e:
                testerror = exception_to_error(e)

            request = grpc_to_normal(request)

            print(request)

            request.success = False
            request.message = "This is a test message"
            request.error = testerror

            data_grpc = normal_to_grpc(request, data_pb2.DataPackage, data_pb2.DataPackageModule, data_pb2.Error) # type: ignore
        except Exception as e:
            print(exception_to_error(e))

        return data_grpc

def serve() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    data_pb2_grpc.add_ModuleServiceServicer_to_server(ModuleServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started on port 50051.")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
