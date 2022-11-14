import sys
import os
import threading
from logging import basicConfig, info, error, DEBUG, INFO
import psutil
from neo4j import GraphDatabase
import time
import datetime


class GraphDriver:
    def __init__(self):
        self._suppressed = False

    def add_node(self, nid: int, labels: list[str], properties: dict):
        raise NotImplementedError

    def add_edge(self, src: str, dst: str, labels: list[str], properties: dict):
        raise NotImplementedError

    def get_single_node(self, labels: list[str], properties: dict):
        raise NotImplementedError

    def load_database(self, path_nodes: str, path_edges: str):
        raise NotImplementedError

    def get_pids(self):
        raise NotImplementedError

    def enter_suppression(self):
        self._suppressed = True

    def exit_suppression(self):
        self._suppressed = False


class NEO4j(GraphDriver):

    def __init__(self, uri, user, password):
        super().__init__()
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        info(f"Neo4j driver connected to {uri}")

    def add_node(self, nid: int, labels: list[str], properties: dict):
        properties.update({"id": nid})
        q = "CREATE (n"
        if labels:
            q += ":" + ":".join(labels)
        q += " {"
        q += ", ".join([f"{k}: \"{v}\"" for k, v in properties.items()])
        q += "}"
        q += ")"
        self.query(q)

    def add_edge(self, src: str, dst: str, labels: list[str], properties: dict):
        q = f"MATCH (a), (b) WHERE a.id = \"{src}\" AND b.id = \"{dst}\" CREATE (a)-[:"
        if labels:
            q += ":".join(labels)
        q += " {"
        q += ", ".join([f"{k}: \"{v}\"" for k, v in properties.items()])
        q += "}"
        q += "]->(b)"
        self.query(q)

    def get_single_node(self, labels: list[str], properties: dict):
        q = "MATCH (n"
        if labels:
            q += ":" + ":".join(labels)
        q += " {"
        q += ", ".join([f"{k}: {v}" for k, v in properties.items()])
        q += "}"
        q += ") RETURN n"
        return self.query(q)

    def load_database(self, path_nodes: str, path_edges: str):
        with open(path_nodes, "r") as f:
            n_properties = f.readline().strip().split(",")
        with open(path_edges, "r") as f:
            e_properties = f.readline().strip().split(",")
        self.query(f"LOAD CSV WITH HEADERS FROM 'file:///{path_nodes}' AS row "
                   f"CREATE (n {{ {', '.join([f'{p}: row.{p}' for p in n_properties])} }})")
        self.query(f"LOAD CSV WITH HEADERS FROM 'file:///{path_edges}' AS row "
                   f"MATCH (a), (b) WHERE a.id = row.src AND b.id = row.dst "
                   f"CREATE (a)-[:{':'.join(e_properties)}]->(b)")

    def close(self):
        self.driver.close()

    def query(self, q):
        with self.driver.session() as session:
            res = None
            if not self._suppressed:
                res = session.run(q)
            return res

    def clear(self):
        self.query("MATCH (n) DETACH DELETE n")

    def get_pids(self):
        return [p.pid for p in psutil.process_iter() if p.name() == "java.exe" and
                "Neo4j Desktop.exe" in [pp.name() for pp in p.parents()]]

    def __str__(self):
        return "NEO4j_DB"


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
                cpu += p.cpu_percent(interval=self._interval)
                mem += p.memory_info().rss
            self._cpu_usage.append(cpu)
            self._memory_usage.append(mem)
            time.sleep(self._interval)

    def get_cpu_usage(self):
        return self._cpu_usage

    def get_memory_usage(self):
        return self._memory_usage

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


def bench_get_single_node(database: GraphDriver, size=1000000):
    info(f"Getting {size} nodes from {database}")
    for i in range(size):
        if database:
            database.get_single_node(labels=["test"], properties={"name": f"test{i}"})


def perform_bench(bench: callable, database, **kwargs):
    info(f"Starting benchmark {bench.__name__} with {database}")
    start = time.time()
    with Suppress(database):
        bench(database, **kwargs)
    end = time.time()
    overhead = end - start
    print(f"Overhead: {overhead}")
    info(f"Overhead is {overhead}")

    profiler = Profiler(database, 0.1)
    start = time.time()
    bench(database, **kwargs)
    end = time.time()
    profiler.stop()
    duration = end - start - overhead
    info(f"Benchmark {bench.__name__} with {database} finished in {end - start}")
    print(profiler.get_summary())
    save_data(f"{bench.__name__}_{database}", profiler.get_cpu_usage(), profiler.get_memory_usage(), duration, 0.1)


def save_data(name, cpu_usage, memory_usage, duration, interval):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    with open(f"Results/{name}_{timestamp}.bench", "w") as f:
        f.write(f"time:{duration}\n")
        f.write("#Time,CPU,Memory\n")
        for i in range(len(cpu_usage)):
            f.write(f"{str(i * interval)},{cpu_usage[i]},{memory_usage[i]}\n")


if __name__ == "__main__":
    basicConfig(filename="benchmark.log", level=INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    d_neo4j = NEO4j("bolt://localhost:7687", "neo4j", "1234")
    d_neo4j.clear()
    perform_bench(bench_add_single_node, d_neo4j)
    perform_bench(bench_add_single_edge, d_neo4j)
