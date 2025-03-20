from __future__ import annotations
from typing import Any, Dict, List, Optional

# basic node in the intermediate graph representation
class GraphNode:
    def __init__(self, node_id: str, node_type: str, params: Dict[str, Any], dependencies: Optional[List[str]] = None):
        self.node_id = node_id
        self.node_type = node_type  # e.g. 'source', 'compute', 'sign', 'sink'
        self.params = params        # parameters such as key, conversion, rate_op, sign, commodity, etc.
        self.dependencies = dependencies if dependencies is not None else []  # list of node_ids this node depends on

# graph that collects nodes and manages validation and compilation
class DSLRuleGraph:
    def __init__(self):
        self.nodes: Dict[str, GraphNode] = {}  # mapping node_id -> GraphNode

    def add_node(self, node: GraphNode):
        if node.node_id in self.nodes:
            raise ValueError(f"node id {node.node_id} already exists")
        self.nodes[node.node_id] = node

    def add_dependency(self, node_id: str, dependency_id: str):
        if node_id not in self.nodes or dependency_id not in self.nodes:
            raise ValueError("both node and dependency must exist")
        self.nodes[node_id].dependencies.append(dependency_id)

    def has_cycle(self) -> bool:
        # simple cycle detection using depth-first search
        visited = {}
        rec_stack = {}

        def dfs(nid: str) -> bool:
            visited[nid] = True
            rec_stack[nid] = True
            for dep_id in self.nodes[nid].dependencies:
                if dep_id not in visited:
                    if dfs(dep_id):
                        return True
                elif rec_stack.get(dep_id, False):
                    return True
            rec_stack[nid] = False
            return False

        for node_id in self.nodes:
            if node_id not in visited:
                if dfs(node_id):
                    return True
        return False

    def validate(self) -> bool:
        # ensure required nodes exist and proper structure exists.
        # required: at least one source node with role 'withdrawal' and one sink node.
        withdrawal_nodes = [n for n in self.nodes.values() if n.node_type == "source" and n.params.get("role") == "withdrawal"]
        if not withdrawal_nodes:
            raise ValueError("graph validation error: missing withdrawal source node")
        sink_nodes = [n for n in self.nodes.values() if n.node_type == "sink"]
        if len(sink_nodes) != 1:
            raise ValueError("graph validation error: there must be exactly one sink node")
        # check acyclicity
        if self.has_cycle():
            raise ValueError("graph validation error: cycle detected in graph")
        # further validations can be added here (e.g. compute node dependencies)
        return True

    def compile_dsl_rule(self) -> dict:
        # derive the final dsl rule from a valid graph
        # mapping:
        # - withdrawal: source node with role 'withdrawal' (its 'key' provides the data key)
        # - rate: optional source node with role 'rate'
        # - compute: node with conversion and rate_op
        # - sign: node providing sign information (default 1 if missing)
        # - sink: node with commodity (and optionally node_prefix)
        sink_nodes = [n for n in self.nodes.values() if n.node_type == "sink"]
        if len(sink_nodes) != 1:
            raise ValueError("compile error: graph must have exactly one sink node")
        sink = sink_nodes[0]

        withdrawal_nodes = [n for n in self.nodes.values() if n.node_type == "source" and n.params.get("role") == "withdrawal"]
        if not withdrawal_nodes:
            raise ValueError("compile error: missing withdrawal source node")
        withdrawal = withdrawal_nodes[0]

        rate_nodes = [n for n in self.nodes.values() if n.node_type == "source" and n.params.get("role") == "rate"]
        compute_nodes = [n for n in self.nodes.values() if n.node_type == "compute"]
        if not compute_nodes:
            raise ValueError("compile error: missing compute node")
        compute = compute_nodes[0]

        sign_nodes = [n for n in self.nodes.values() if n.node_type == "sign"]
        sign = sign_nodes[0] if sign_nodes else None

        dsl_rule = {
            "commodity": sink.params.get("commodity"),
            "withdrawal": withdrawal.params.get("key"),
            "conversion": compute.params.get("conversion"),
            "rate_op": compute.params.get("rate_op"),
            "sign": sign.params.get("sign") if sign is not None else 1,
        }
        dsl_rule["rate"] = rate_nodes[0].params.get("key") if rate_nodes else None
        return dsl_rule

# demonstration of the intermediate graph process
if __name__ == "__main__":
    # simulate user input conversion into graph nodes.
    graph = DSLRuleGraph()

    # node for withdrawal source (user indicates the key to withdrawal data)
    node_withdrawal = GraphNode(node_id="n1", node_type="source", params={"role": "withdrawal", "key": "urban_withdrawal"})
    graph.add_node(node_withdrawal)

    # optional node for rate source (user indicates the key to rate data)
    node_rate = GraphNode(node_id="n2", node_type="source", params={"role": "rate", "key": "urban_connection_rate"})
    graph.add_node(node_rate)

    # compute node combining withdrawal and rate (with parameters for conversion and rate_op)
    node_compute = GraphNode(node_id="n3", node_type="compute", params={"conversion": 1e-3, "rate_op": "identity"}, dependencies=["n1", "n2"])
    graph.add_node(node_compute)

    # sign node that adjusts the computed value (user can specify sign here)
    node_sign = GraphNode(node_id="n4", node_type="sign", params={"sign": 1}, dependencies=["n3"])
    graph.add_node(node_sign)

    # sink node that packages the result into the final duct format, including commodity info
    node_sink = GraphNode(node_id="n5", node_type="sink", params={"commodity": "urban_mw", "node_prefix": "B"}, dependencies=["n1", "n4"])
    graph.add_node(node_sink)

    # validate the intermediate graph (raises error if something is wrong)
    if graph.validate():
        print("graph valid")
    # compile/derive the final dsl rule dictionary from the graph
    derived_rule = graph.compile_dsl_rule()
    print("derived dsl rule:")
    print(derived_rule)