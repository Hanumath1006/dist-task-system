import hashlib
import os
import threading
import time
import uuid
from concurrent import futures

import grpc

from generated import task_pb2, task_pb2_grpc

LOCK = threading.Lock()
RESULTS = {}


def fib(n: int) -> int:
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


def process(task_type, payload):
    if task_type == "sha256":
        return hashlib.sha256(payload.encode()).hexdigest()

    elif task_type == "reverse":
        return payload[::-1]

    elif task_type == "fib":
        n = int(payload)
        if n < 0 or n > 45:
            raise ValueError("fib input must be 0-45")
        return str(fib(n))

    else:
        raise ValueError("unknown task type")


class Node(task_pb2_grpc.ApiServiceServicer):

    def Ping(self, request, context):
        return task_pb2.PingResponse(message="node:ok")

    def SubmitTask(self, request, context):
        task_id = str(uuid.uuid4())

        try:
            result = process(request.task_type, request.payload)
            status = "DONE"
            error = ""
        except Exception as e:
            result = ""
            status = "ERROR"
            error = str(e)

        with LOCK:
            RESULTS[task_id] = {
                "status": status,
                "result": result,
                "error": error
            }

        return task_pb2.SubmitTaskResponse(task_id=task_id)

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
    port = os.environ.get("PORT", "50050")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    task_pb2_grpc.add_ApiServiceServicer_to_server(Node(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[MONOLITH NODE] running on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
    