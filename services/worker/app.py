import hashlib
import os
import time
from concurrent import futures

import grpc

from generated import task_pb2, task_pb2_grpc


def fib(n: int) -> int:
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


class Worker(task_pb2_grpc.WorkerServiceServicer):

    def Ping(self, request, context):
        return task_pb2.PingResponse(message="worker:ok")

    def ProcessTask(self, request, context):
        start = time.time()

        try:
            if request.task_type == "sha256":
                result = hashlib.sha256(
                    request.payload.encode("utf-8")
                ).hexdigest()

            elif request.task_type == "reverse":
                result = request.payload[::-1]

            elif request.task_type == "fib":
                n = int(request.payload)
                if n < 0 or n > 45:
                    raise ValueError("fib input must be 0-45")
                result = str(fib(n))

            else:
                raise ValueError("unknown task type")

            elapsed = int((time.time() - start) * 1000)

            return task_pb2.ProcessTaskResponse(
                task_id=request.task_id,
                status="DONE",
                result=result,
                error="",
                worker_ms=elapsed
            )

        except Exception as e:
            elapsed = int((time.time() - start) * 1000)

            return task_pb2.ProcessTaskResponse(
                task_id=request.task_id,
                status="ERROR",
                result="",
                error=str(e),
                worker_ms=elapsed
            )


def serve():
    port = os.environ.get("PORT", "50052")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    task_pb2_grpc.add_WorkerServiceServicer_to_server(Worker(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[WORKER] running on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()

