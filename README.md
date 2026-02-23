### Overview

This project implements a distributed task processing system using Python, gRPC, and Docker.

Two architectures are provided:

Microservices architecture

Monolith-per-node architecture

### Requirements

Docker Desktop

Python 3.9

pip

matplotlib (for plotting)


### Generate Protobuf Files

From project root: python -m grpc_tools.protoc -I proto --python_out=generated --grpc_python_out=generated proto/task.proto

### Run Microservices Architecture

Build and run: docker compose -f docker-compose.micro.yml up --build

Services:

API: localhost:50050

Scheduler

Store

3 Worker nodes

### Run Monolith Architecture

Stop micro: docker compose -f docker-compose.micro.yml down

Run monolith: docker compose -f docker-compose.mono.yml up --build

Nodes:
localhost:50060–50065

### Benchmarking

Run benchmark: python client\bench.py --addr localhost:50050 --total 2000 --concurrency 50 --out micro_50.csv

For monolith: python client\bench.py --addr localhost:50060 --total 2000 --concurrency 50 --out mono_50.csv

Generate Graphs

Install matplotlib:

pip install matplotlib

### Generate graphs:

python client\plot.py --out results

Outputs:

results_latency.png

results_throughput.png
