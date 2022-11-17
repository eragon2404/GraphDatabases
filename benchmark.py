import sys
import os
import threading
import logging
import psutil
import time
import datetime
from databases import GraphDriver, NEO4j, ArangoDB, OrientDB


class Suppress:
    def __init__(self, database: GraphDriver):
        self.database = database

    def __enter__(self):
        self.database.enter_suppression()
        info(f"Entering suppression mode for {self.database}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.database.exit_suppression()
        info(f"Exiting suppression mode for {self.database}")


class Profiler:
    def __init__(self, database: GraphDriver, interval, auto_start=True):
        self._pids = database.get_pids()
        if not self._pids:
            error(f"No PIDs found for {database}")
        self._interval = interval
        self._thread = None
        self._running = False
        self._cpu_usage = []
        self._memory_usage = []

        info(f"Profiler initialized for {database} with pids {self._pids}")
        if auto_start:
            self.start()

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()
        info(f"Profiler started")

    def stop(self):
        self._running = False
        self._thread.join()
        info(f"Profiler stopped")

    def _run(self):
        while self._running:
            cpu = 0
            mem = 0
            for pid in self._pids:
                p = psutil.Process(pid)
                cpu += p.cpu_percent(interval=1)
                mem += p.memory_info().rss
            self._cpu_usage.append(cpu)
            self._memory_usage.append(mem)
            time.sleep(self._interval)

    def get_cpu_usage(self):
        return self._cpu_usage

    def get_memory_usage(self):
        return self._memory_usage

    def get_average_cpu_usage(self):
        return sum(self._cpu_usage) / len(self._cpu_usage)

    def get_average_memory_usage(self):
        return sum(self._memory_usage) / len(self._memory_usage) / 1024 / 1024

    def get_summary(self):
        return f"CPU average: {sum(self._cpu_usage) / len(self._cpu_usage)} %, " \
               f"MEM average: {sum(self._memory_usage) / len(self._memory_usage) / 1024 / 1024} MB"


def bench_add_single_node(database: GraphDriver, size=10000):
    info(f"Adding {size} nodes to {database}")
    for i in range(size):
        database.add_node(nid=i, labels=["test"], properties={"name": f"test{i}"})


def bench_add_single_edge(database: GraphDriver, size=1000):
    info(f"Adding {size} edges to {database}")
    for i in range(size - 1):
        database.add_edge(src=f"{i}", dst=f"{i + 1}", labels=["test"], properties={"name": f"test{i}"})


def bench_add_database(database: GraphDriver, path_node: str, path_edge: str):
    info(f"Adding database from {path_node} and {path_edge} to {database}")
    if database:
        database.load_database(path_node, path_edge)


def bench_get_single_node(database: GraphDriver, size=1000):
    info(f"Getting {size} nodes from {database}")
    for i in range(size):
        if database:
            database.get_single_node(labels=["test"], properties={"name": f"test{i}"})


def perform_bench(bench: callable, database, save=True, **kwargs):
    info(f"Starting benchmark {bench.__name__} with {database}")
    start = time.time()
    with Suppress(database):
        bench(database, **kwargs)
    end = time.time()
    overhead = end - start
    info(f"Overhead is {overhead}")

    profiler = Profiler(database, 0.1)
    start = time.time()
    bench(database, **kwargs)
    end = time.time()
    profiler.stop()
    duration = end - start - overhead
    info(f"Benchmark {bench.__name__} with {database} finished in {duration}")
    info(profiler.get_summary())
    if save:
        save_data(f"{bench.__name__}_{database}", [i * 1.1 for i in range(len(profiler.get_average_cpu_usage()))],
                  profiler.get_cpu_usage(), profiler.get_memory_usage())
    else:
        return profiler.get_average_cpu_usage(), profiler.get_average_memory_usage(), duration


def iterate_bench(bench: callable, database, **kwargs):
    values = []
    cpu_usage = []
    memory_usage = []
    duration = []
    v_name = None
    for arg in kwargs:
        if type(kwargs[arg]) is list:
            v_name = arg
            for value in kwargs[arg]:
                new_kwargs = kwargs.copy()
                new_kwargs[arg] = value
                res = perform_bench(bench, database, save=False, **new_kwargs)
                values.append(value)
                cpu_usage.append(res[0])
                memory_usage.append(res[1])
                duration.append(res[2])
            break
    save_data(f"{bench.__name__}_{database}_iter", ["_" + v_name, "CPU", "MEM", "TIME"], values, cpu_usage,
              memory_usage, duration)


def save_data(name, head, *args):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    name = f"Results/{name}_{timestamp}.bench"
    info(f"Saving data to {name}")
    with open(name, "w") as f:
        f.write(",".join(head) + "\n")
        for i in range(len(args[0])):
            for arg in args:
                f.write(f"{arg[i]},")
            f.write("\n")


if __name__ == "__main__":
    logging.basicConfig(filename="benchmark.log", level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    info = logging.info
    error = logging.error
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    do_neo4j = False
    do_arango = False
    do_orient = True
    d_neo4j = None
    d_arango = None
    d_orient = None

    if do_neo4j:
        try:
            d_neo4j = NEO4j("bolt://localhost:7687", "neo4j", "1234")
            d_neo4j.clear()
        except Exception as e:
            error(f"Could not connect to Neo4j: {e}")

    if do_arango:
        try:
            d_arango = ArangoDB("http://localhost:8529", "root", "arango")
            d_arango.clear()
        except Exception as e:
            error(f"Could not connect to ArangoDB: {e}")

    if do_orient:
        try:
            d_orient = OrientDB("localhost", "root", "orient")
            d_orient.clear()
        except Exception as e:
            error(f"Could not connect to OrientDB: {e}")
            d_orient = None

    if d_neo4j:
        perform_bench(bench_add_single_node, d_neo4j, size=10000)
        perform_bench(bench_add_single_edge, d_neo4j, size=1000)
        # perform_bench(bench_add_database, d_neo4j, path_node="data/nodes.csv", path_edge="data/edges.csv")
        perform_bench(bench_get_single_node, d_neo4j, size=1000)

    if d_arango:
        perform_bench(bench_add_single_node, d_arango, size=10000)
        perform_bench(bench_add_single_edge, d_arango, size=1000)
        # perform_bench(bench_add_database, d_arango, path_node="data/nodes.csv", path_edge="data/edges.csv")
        perform_bench(bench_get_single_node, d_arango, size=1000)

    if d_orient:
        iterate_bench(bench_add_single_node, d_orient, size=[i * 10000 for i in range(1, 11)])

        # perform_bench(bench_add_single_node, d_orient, size=10000)
        # perform_bench(bench_add_single_edge, d_orient, size=1000)
        # perform_bench(bench_add_database, d_orient, path_node="data/nodes.csv", path_edge="data/edges.csv")
        # perform_bench(bench_get_single_node, d_orient, size=1000)
