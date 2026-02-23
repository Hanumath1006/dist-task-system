import os
import uuid
from concurrent import futures

import grpc

from generated import task_pb2, task_pb2_grpc

WORKERS = os.environ.get(
    "WORKERS",
    "worker1:50052,worker2:50052,worker3:50052"
).split(",")

STORE_ADDR = os.environ.get("STORE_ADDR", "store:50053")

_rr_index = 0


def get_next_worker():
    global _rr_index
    worker = WORKERS[_rr_index % len(WORKERS)]
    _rr_index += 1
    return worker


class Scheduler(task_pb2_grpc.SchedulerServiceServicer):

    def Ping(self, request, context):
        return task_pb2.PingResponse(message="scheduler:ok")

    def SubmitTask(self, request, context):
        task_id = str(uuid.uuid4())
        worker_addr = get_next_worker()

        try:
            with grpc.insecure_channel(worker_addr) as channel:
                worker = task_pb2_grpc.WorkerServiceStub(channel)
                response = worker.ProcessTask(
                    task_pb2.ProcessTaskRequest(
                        task_id=task_id,
                        task_type=request.task_type,
                        payload=request.payload
                    ),
                    timeout=5.0
                )

        except Exception as e:
            response = task_pb2.ProcessTaskResponse(
                task_id=task_id,
                status="ERROR",
                result="",
                error=str(e),
                worker_ms=0
            )

        with grpc.insecure_channel(STORE_ADDR) as store_channel:
            store = task_pb2_grpc.StoreServiceStub(store_channel)
            store.PutResult(
                task_pb2.PutResultRequest(
                    task_id=response.task_id,
                    status=response.status,
                    result=response.result,
                    error=response.error
                )
            )

        return task_pb2.SubmitTaskResponse(task_id=task_id)


def serve():
    port = os.environ.get("PORT", "50051")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    task_pb2_grpc.add_SchedulerServiceServicer_to_server(Scheduler(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[SCHEDULER] running on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()