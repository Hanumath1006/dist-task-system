import os
import threading
import time
from concurrent import futures

import grpc

from generated import task_pb2, task_pb2_grpc

LOCK = threading.Lock()
RESULTS = {}

class Store(task_pb2_grpc.StoreServiceServicer):

    def Ping(self, request, context):
        return task_pb2.PingResponse(message="store:ok")

    def PutResult(self, request, context):
        with LOCK:
            RESULTS[request.task_id] = {
                "status": request.status,
                "result": request.result,
                "error": request.error,
                "updated_at": time.time(),
            }
        return task_pb2.PutResultResponse(ok=True)

    def GetResult(self, request, context):
        with LOCK:
            record = RESULTS.get(request.task_id)

        if not record:
            return task_pb2.GetResultResponse(
                task_id=request.task_id,
                status="NOT_FOUND",
                result="",
                error=""
            )

        return task_pb2.GetResultResponse(
            task_id=request.task_id,
            status=record["status"],
            result=record["result"],
            error=record["error"]
        )


def serve():
    port = os.environ.get("PORT", "50053")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    task_pb2_grpc.add_StoreServiceServicer_to_server(Store(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[STORE] running on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()