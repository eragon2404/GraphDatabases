import psutil
from neo4j import GraphDatabase
from pyArango.connection import *
import pyorient


class GraphDriver:
    def __init__(self):
        self._suppressed = False

    def add_node(self, nid: int, labels: list[str], properties: dict):
        """
        Add a node to the database
        :param nid: The id of the node
        :param labels: The labels of the node
        :param properties: The properties of the node
        """
        raise NotImplementedError

    def add_edge(self, src: str, dst: str, labels: list[str], properties: dict):
        """
        Add an edge to the database
        :param src: The source node
        :param dst: The destination node
        :param labels: The labels of the edge
        :param properties: The properties of the edge
        """
        raise NotImplementedError

    def get_single_node(self, labels: list[str], properties: dict):
        """
        Get a single node that matches the given labels and properties
        :param labels: The labels of the node
        :param properties: The properties of the node
        """
        raise NotImplementedError

    def get_nodes_hops(self, node_id: int, hops: int):
        """
        Get all nodes that are at most hops away from the given node
        :param node_id: The id of the node
        :param hops: The number of hops
        """
        raise NotImplementedError

    def ssp(self, src: int, dst: int):
        """
        Get the shortest path between two nodes
        :param src: The source node
        :param dst: The destination node
        """
        raise NotImplementedError

    def load_database(self, path_nodes: str, path_edges: str):
        """
        Load the database from the given files
        :param path_nodes: The path to the nodes file
        :param path_edges: The path to the edges file
        """
        raise NotImplementedError

    def get_pids(self):
        """
        Get the pids of the processes that are related to the database
        """
        raise NotImplementedError

    def enter_suppression(self):
        """
        Enter suppression mode. The driver will not send any queries to the database.
        """
        self._suppressed = True

    def exit_suppression(self):
        """
        Exit suppression mode. The driver will send queries to the database again.
        """
        self._suppressed = False


class NEO4j(GraphDriver):
    """
    Driver for Neo4j
    """

    def __init__(self, uri, user, password):
        super().__init__()
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

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
        q += ", ".join([f"{k}: \"{v}\"" for k, v in properties.items()])
        q += "}"
        q += ") RETURN n"
        return self.query(q)

    def get_nodes_hops(self, node_id, hops):
        q = f"MATCH (n)-[*1..{hops}]->(m) WHERE n.id = \"{node_id}\" RETURN DISTINCT m"
        return self.query(q)

    def ssp(self, src, dst):
        q = f"MATCH p=shortestPath((a)-[*]->(b)) WHERE a.id = \"{src}\" AND b.id = \"{dst}\" RETURN p"
        return self.query(q)

    def load_database(self, path_nodes: str, path_edges: str):
        with open(path_nodes, "r") as f:
            for line in f:
                nid = int(line.strip())
                self.add_node(nid, ["test"], {"test": "test"})
        with open(path_edges, "r") as f:
            for line in f:
                src, dst = line.strip().split("\t")
                self.add_edge(src, dst, ["test"], {"test": "test"})

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
        return "NEO4j"


class ArangoDB(GraphDriver):
    """
    Driver for ArangoDB
    """
    def __init__(self, uri, user, password):
        super().__init__()
        self.conn = Connection(username=user, password=password, arangoURL=uri)
        if "benchmark" in self.conn.databases:
            self.db = self.conn["benchmark"]
        else:
            self.db = self.conn.createDatabase(name="benchmark")
        if "nodes" not in self.db.collections:
            self.db.createCollection(name="nodes")
        if "edges" not in self.db.collections:
            self.db.createCollection(name="edges", className="Edges")
        if "benchgraph" not in self.db.graphs:
            self.query("GRAPH_CREATE('benchgraph', ['nodes', 'edges'])")

    def query(self, q):
        res = None
        if not self._suppressed:
            res = self.db.AQLQuery(q)
        return res

    def clear(self):
        self.query("FOR n IN nodes REMOVE n IN nodes")
        self.query("FOR n IN edges REMOVE n IN edges")

    def add_node(self, nid: int, labels: list[str], properties: dict):
        # add node to arango
        properties.update({"id": nid})
        q = "INSERT {"
        q += ", ".join([f"{k}: \"{v}\"" for k, v in properties.items()])
        q += "}"
        q += " INTO "
        q += "nodes"
        self.query(q)

    def add_edge(self, src: str, dst: str, labels: list[str], properties: dict):
        # add edge from src to dst to arango
        q = f"FOR a IN nodes FILTER a.id == \"{src}\" FOR b IN nodes FILTER b.id == \"{dst}\" "
        q += "INSERT { _from: a._id, _to: b._id, "
        q += ", ".join([f"{k}: \"{v}\"" for k, v in properties.items()])
        q += "}"
        q += " INTO "
        q += "edges"
        self.query(q)

    def get_single_node(self, labels: list[str], properties: dict):
        q = "FOR n IN nodes FILTER "
        q += " AND ".join([f"n.{k} == \"{v}\"" for k, v in properties.items()])
        q += " RETURN n"
        return self.query(q)

    def load_database(self, path_nodes: str, path_edges: str):
        with open(path_nodes, "r") as f:
            for line in f:
                nid = int(line.strip())
                self.add_node(nid, ["test"], {"test": "test"})
        with open(path_edges, "r") as f:
            for line in f:
                src, dst = line.strip().split("\t")
                self.add_edge(src, dst, ["test"], {"test": "test"})

    def get_nodes_hops(self, node_id, hops):
        q = f"LET vertex = (FOR v IN nodes FILTER v.id == \"{node_id}\" RETURN v) FOR v, e, p IN 1..{hops} " \
            f"OUTBOUND vertex[0] GRAPH 'benchgraph' OPTIONS {{order:\"bfs\", uniqueVertices:\"global\"}} RETURN DISTINCT v"
        return self.query(q)

    def ssp(self, src, dst):
        q = f"LET start = (FOR v IN nodes FILTER v.id == \"{src}\" RETURN v) " \
            f"LET end = (FOR v IN nodes FILTER v.id == \"{dst}\" RETURN v) " \
            f"FOR p IN OUTBOUND SHORTEST_PATH start[0] TO end[0] GRAPH benchgraph RETURN p"
        return self.query(q)

    def get_pids(self):
        return [p.pid for p in psutil.process_iter() if p.name() == "arangod.exe"]

    def __str__(self):
        return "ArangoDB"


class OrientDB(GraphDriver):
    """
    Driver for OrientDB
    """
    def __init__(self, uri, user, password):
        super().__init__()
        self.client = pyorient.OrientDB(uri, 2424)
        self.client.set_session_token(True)
        self.session_id = self.client.connect(user, password)
        if "benchmark" not in self.client.db_list().oRecordData["databases"].keys():
            self.client.db_create("benchmark", pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
        self.client.db_open("benchmark", user, password)

    def query(self, q):
        res = None
        if not self._suppressed:
            try:
                res = self.client.command(q)
            except pyorient.exceptions.PyOrientCommandException:
                pass
            except TimeoutError:
                print("Timeout")
        return res

    def clear(self):
        self.query("DELETE VERTEX V")
        self.query("DELETE EDGE E")

    def add_node(self, nid: int, labels: list[str], properties: dict):
        # add node to orient
        properties.update({"id": nid})
        q = f"CREATE VERTEX V content {properties}"
        self.query(q)

    def add_edge(self, src: str, dst: str, labels: list[str], properties: dict):
        # add edge from src to dst to orient
        q = f"CREATE EDGE E FROM (SELECT FROM V WHERE id = \"{src}\") TO (SELECT FROM V WHERE id = \"{dst}\")"
        q += f" content {properties}"
        self.query(q)

    def get_single_node(self, labels: list[str], properties: dict):
        q = "SELECT FROM V WHERE "
        q += " AND ".join([f"{k} = \"{v}\"" for k, v in properties.items()])
        return self.query(q)

    def get_nodes_hops(self, node_id, hops):
        q = f"TRAVERSE OUT() FROM (SELECT FROM V WHERE id = \"{node_id}\") MAXDEPTH {hops}"
        return self.query(q)

    def ssp(self, src, dst):
        q = f"SELECT shortestPath((SELECT FROM V WHERE id = \"{src}\"), (SELECT FROM V WHERE id = \"{dst}\"))"
        return self.query(q)

    def load_database(self, path_nodes: str, path_edges: str):
        with open(path_nodes, "r") as f:
            for line in f:
                nid = int(line.strip())
                self.add_node(nid, ["test"], {"test": "test"})
        with open(path_edges, "r") as f:
            for line in f:
                src, dst = line.strip().split("\t")
                self.add_edge(src, dst, ["test"], {"test": "test"})

    def get_pids(self):
        return [p.pid for p in psutil.process_iter() if p.name() == "java.exe" and
                [item for item in p.cmdline() if "orientdb" in item]]

    def __str__(self):
        return "OrientDB"
