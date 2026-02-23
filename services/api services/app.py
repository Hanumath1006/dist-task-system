import os
from concurrent import futures

import grpc

from generated import task_pb2, task_pb2_grpc

SCHEDULER_ADDR = os.environ.get("SCHEDULER_ADDR", "scheduler:50051")
STORE_ADDR = os.environ.get("STORE_ADDR", "store:50053")


class Api(task_pb2_grpc.ApiServiceServicer):

    def Ping(self, request, context):
        return task_pb2.PingResponse(message="api:ok")

    def SubmitTask(self, request, context):
        with grpc.insecure_channel(SCHEDULER_ADDR) as channel:
            scheduler = task_pb2_grpc.SchedulerServiceStub(channel)
            return scheduler.SubmitTask(request)

    def GetResult(self, request, context):
        with grpc.insecure_channel(STORE_ADDR) as channel:
            store = task_pb2_grpc.StoreServiceStub(channel)
            return store.GetResult(request)


def serve():
    port = os.environ.get("PORT", "50050")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    task_pb2_grpc.add_ApiServiceServicer_to_server(Api(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[API] running on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()

    