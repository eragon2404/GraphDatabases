import sys
import os
import threading
import logging
import psutil
import time
import datetime
from databases import GraphDriver, NEO4j, ArangoDB, OrientDB
from tkinter import *


class Suppress:
    """
    Forces the database to enter suppression mode, which means that no queries will be executed.
    """
    def __init__(self, database: GraphDriver):
        self.database = database

    def __enter__(self):
        self.database.enter_suppression()
        info(f"Entering suppression mode for {self.database}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.database.exit_suppression()
        info(f"Exiting suppression mode for {self.database}")


class Profiler:
    """
    Monitors the CPU and memory usage of the database.
    """
    def __init__(self, database: GraphDriver, interval, auto_start=True):
        """
        :param database: The database to monitor
        :param interval: The interval in seconds between each measurement
        :param auto_start: Whether to start the profiler automatically
        """
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
        def cpu_thread(index: int, proc, interval):
            nonlocal cpu
            cpu[index] = proc.cpu_percent(interval=interval)

        while self._running:
            cpu_thread_pool = []
            cpu = [0 for _ in range(len(self._pids))]
            mem = 0

            for pid in self._pids:
                p = psutil.Process(pid)
                cpu_thread_pool.append(threading.Thread(target=cpu_thread, args=(self._pids.index(pid), p, 0.9)))
                mem += p.memory_info().rss

            for t in cpu_thread_pool:
                t.start()
            for t in cpu_thread_pool:
                t.join()
            self._cpu_usage.append(sum(cpu))
            self._memory_usage.append(mem / 1024 / 1024)
            time.sleep(self._interval)

    def get_cpu_usage(self):
        return self._cpu_usage

    def get_memory_usage(self):
        return self._memory_usage

    def get_average_cpu_usage(self):
        return sum(self._cpu_usage) / len(self._cpu_usage)

    def get_average_memory_usage(self):
        return sum(self._memory_usage) / len(self._memory_usage)

    def get_summary(self):
        return f"CPU average: {sum(self._cpu_usage) / len(self._cpu_usage)} %, " \
               f"MEM average: {sum(self._memory_usage) / len(self._memory_usage)} MB"


def bench_add_single_node(database: GraphDriver, size=10000):
    """
    Adds size nodes to the database
    :param database: The database to add the nodes to
    :param size: Number of nodes to add
    """
    info(f"Adding {size} nodes to {database}")
    for i in range(size):
        database.add_node(nid=i, labels=["test"], properties={"name": f"test{i}"})


def bench_add_single_edge(database: GraphDriver, size=1000):
    """
    Adds size edges to the database
    :param database: The database to add the edges to
    :param size: Number of edges to add
    """
    info(f"Adding {size} edges to {database}")
    for i in range(size - 1):
        database.add_edge(src=f"{i}", dst=f"{i + 1}", labels=["test"], properties={"name": f"test{i}"})


def bench_add_database(database: GraphDriver, path_node: str = "data_sets/Wiki-VoteN.txt",
                       path_edge: str = "data_sets/Wiki-VoteE.txt"):
    """
    Adds the nodes and edges from the given files to the database
    :param database: The database to add the nodes and edges to
    :param path_node: Path to the file containing the nodes
    :param path_edge: Path to the file containing the edges
    """
    info(f"Adding database from {path_node} and {path_edge} to {database}")
    if database:
        database.load_database(path_node, path_edge)


def bench_get_single_node(database: GraphDriver, size=1000):
    """
    queries size nodes from the database
    :param database: The database to query the nodes from
    :param size: Number of nodes to query
    """
    info(f"Getting {size} nodes from {database}")
    for i in range(size):
        if database:
            database.get_single_node(labels=["test"], properties={"name": f"test{i}"})


def create_gird_graph(database: GraphDriver, size=150):
    """
    Creates a grid graph with size * size nodes
    :param database: The database to add the nodes and edges to
    :param size: The size of the grid
    """
    info(f"Creating grid graph with {size} nodes in {database}")
    for i in range(size ** 2):
        database.add_node(nid=i, labels=["test"], properties={"name": f"test{i}"})
    for i in range(size ** 2):

        if i % size != size - 1:
            database.add_edge(src=f"{i}", dst=f"{i + 1}", labels=["test"], properties={"name": f"test{i}"})

        if i < size ** 2 - size:
            database.add_edge(src=f"{i}", dst=f"{i + size}", labels=["test"], properties={"name": f"test{i}"})


def bench_traversal(database: GraphDriver, start_node=1, size=10):
    """
    Traverses the graph starting at start_node with size steps
    :param database: The database to traverse
    :param start_node: The node to start the traversal at
    :param size: The number of steps to take
    """
    hops = size
    info(f"Starting traversal from {start_node} with {hops} hops in {database}")
    if database:
        database.get_nodes_hops(start_node, hops)


def bench_spp(database: GraphDriver, start_node=1, size=10):
    """
    Calculates the shortest path from start_node to all other nodes in the graph
    :param database: The database to calculate the shortest path in
    :param start_node: The node to start the shortest path at
    :param size: The number of nodes to calculate the shortest path to
    """
    info(f"Starting shortest path from {start_node} with length {size} in {database}")
    if database:
        database.ssp(start_node, 151 * size)


def bench_idle_usage(database: GraphDriver, duration=60):
    """
    Does absolutely nothing for the given duration
    :param database: The database to do nothing with
    :param duration: The duration to do nothing for
    """
    info(f"Starting idle usage for {duration} seconds with {database}")
    time.sleep(duration)


def perform_bench(bench: callable, database, save=True, **kwargs):
    """
    Performs the given benchmark on the given database
    :param bench: The benchmark to perform
    :param database: The database to perform the benchmark on
    :param save: Whether to save the results
    :param kwargs: The arguments to pass to the benchmark
    :return: The results of the benchmark
    """
    # Get overhead
    info(f"Starting benchmark {bench.__name__} with {database}")
    start = time.time()
    with Suppress(database):
        bench(database, **kwargs)
    end = time.time()
    overhead = end - start
    info(f"Overhead is {overhead}")

    # Perform benchmark
    profiler = Profiler(database, 0.1)
    start = time.time()
    bench(database, **kwargs)
    end = time.time()
    profiler.stop()
    duration = end - start - overhead
    info(f"Benchmark {bench.__name__} with {database} finished in {duration}")
    info(profiler.get_summary())
    if save:
        save_data(f"{bench.__name__}_{database}", ["_Time [s]", "CPU [%]", "MEM [MB]"],
                  [i for i in range(len(profiler.get_cpu_usage()))],
                  profiler.get_cpu_usage(), profiler.get_memory_usage())
    else:
        return profiler.get_average_cpu_usage(), profiler.get_average_memory_usage(), duration


def iterate_bench(bench: callable, database, **kwargs):
    """
    Iterates the given benchmark on the given database
    :param bench: The benchmark to iterate
    :param database: The database to iterate the benchmark on
    :param kwargs: One kwarg must be a list of values to iterate over. Rest is passed to the benchmark.
    """
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
    save_data(f"{bench.__name__}_{database}_iter", ["_" + v_name, "CPU [%]", "MEM [MB]", "TIME [s]"], values, cpu_usage,
              memory_usage, duration)


def save_data(name, head, *args):
    """
    Saves the given data to a csv file
    :param name: The name of the file
    :param head: The header of the csv file
    :param args: The data to save
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    name = f"Results/{name}_{timestamp}.bench"
    info(f"Saving data to {name}")
    with open(name, "w") as f:
        f.write(",".join(head) + "\n")
        for i in range(len(args[0])):
            for arg in args:
                f.write(f"{arg[i]},")
            f.write("\n")


def selection_window():
    """
    Creates a window to select the benchmark and databases to run.
    :return: The selected benchmark, databases, whether to iterate,
    the amount of steps and a factor and whether to clear the databases
    """
    benchmarks = [bench_add_single_node, bench_add_single_edge, bench_add_database, bench_get_single_node,
                  bench_idle_usage, bench_traversal, create_gird_graph, bench_spp]
    databases = [NEO4j, OrientDB, ArangoDB]
    root = Tk()
    root.title("Benchmark")
    # root.geometry("500x500")
    left = Frame(root, border=1, relief=SUNKEN)
    left.pack(side=LEFT, fill=BOTH, expand=True)
    right = Frame(root, border=1, relief=SUNKEN)
    right.pack(side=RIGHT, fill=BOTH, expand=True)
    Label(left, text="Databases").pack()
    Label(right, text="Benchmark").pack()
    selected_bench = StringVar(root)
    selected_bench.set(benchmarks[0].__name__)
    OptionMenu(right, selected_bench, *[b.__name__ for b in benchmarks]).pack()
    selected_dbs = []
    for db in databases:
        selected_dbs.append(IntVar())
        Checkbutton(left, text=db.__str__(None), variable=selected_dbs[-1]).pack()
    Button(left, text="Start", command=root.destroy).pack(side=BOTTOM, fill=X)

    def iterate_cb():
        if iterate.get():
            b_steps.config(state=NORMAL)
            b_factor.config(state=NORMAL)
        else:
            b_steps.config(state=DISABLED)
            b_factor.config(state=DISABLED)

    iterate = IntVar()
    b_iterate = Checkbutton(right, text="Iterate", variable=iterate, command=iterate_cb)
    b_iterate.pack()

    Label(right, text="Number of Steps:").pack()
    steps = IntVar()
    b_steps = Entry(right, textvariable=steps, state=DISABLED)
    b_steps.pack()

    Label(right, text="Factor:").pack()
    factor = IntVar()
    b_factor = Entry(right, textvariable=factor, state=DISABLED)
    b_factor.pack()

    clear = IntVar()
    Checkbutton(left, text="Clear", variable=clear).pack()

    root.mainloop()
    return selected_bench.get(), [databases[i] for i in range(len(databases)) if selected_dbs[i].get()], \
           iterate.get(), steps.get(), factor.get(), clear.get()


if __name__ == "__main__":
    logging.basicConfig(filename="benchmark.log", level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    info = logging.info
    error = logging.error
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    settings = selection_window()

    d_neo4j = None
    d_arango = None
    d_orient = None
    for db in settings[1]:
        if db == NEO4j:
            try:
                d_neo4j = NEO4j("bolt://localhost:7687", "neo4j", "1234")
                if settings[5]:
                    d_neo4j.clear()
            except Exception as e:
                error(f"Could not connect to Neo4j: {e}")
        elif db == ArangoDB:
            try:
                d_arango = ArangoDB("http://localhost:8529", "root", "arango")
                if settings[5]:
                    d_arango.clear()
            except Exception as e:
                error(f"Could not connect to ArangoDB: {e}")
        elif db == OrientDB:
            try:
                d_orient = OrientDB("localhost", "root", "orient")
                if settings[5]:
                    d_orient.clear()
            except Exception as e:
                error(f"Could not connect to OrientDB: {e}")

    for db in [d_neo4j, d_arango, d_orient]:
        if db is not None:
            if settings[2]:
                iterate_bench(globals()[settings[0]], db, size=[i * settings[4] for i in range(1, settings[3] + 1)])
            else:
                perform_bench(globals()[settings[0]], db)
