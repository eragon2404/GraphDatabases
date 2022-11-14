from neo4j import GraphDatabase


class neo4j_driver:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def query(self, q):
        with self.driver.session() as session:
            res = session.run(q)
            return res

    def clear(self):
        self.query("MATCH (n) DETACH DELETE n")


n = neo4j_driver("bolt://localhost:7687", "neo4j", "1234")
n.clear()

q_merge_node = "MERGE (n {{name: \"{0}\"}})"
q_create_rel = "MATCH (a), (b) WHERE a.name = \"{0}\" AND b.name = \"{1}\" CREATE (a)-[:know]->(b)"

with open("data_sets/WikiTalk.txt") as file:
    for line in file:
        if line[0] == "#":
            continue
        a, b = line.split()
        print(a, b)
        n.query(q_merge_node.format(a))
        n.query(q_merge_node.format(b))
        n.query(q_create_rel.format(a, b))


def measure_ram():
    # measure ram usage
    l = [i for i in range(10000000)]
    process = psutil.Process(os.getpid())
    return process.memory_info().rss


def measure_cpu():
    # measure cpu usage of this process
    process = psutil.Process(os.getpid())
    return process.cpu_percent(interval=1)


print(measure_ram())
print(measure_cpu())