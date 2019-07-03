#!/usr/bin/env python3

import collections
import itertools
import sys
import os
from pathlib import Path
import re

from graphviz import Digraph

EXTRACT_NODES = re.compile(r"JOB\s+(?P<name>\S+)\s+(?P<file>\S+)")
EXTRACT_EDGES = re.compile(r"PARENT\s+(?P<parents>.+)\s+CHILD\s+(?P<children>.+)")


def get_nodes_and_edges(dagfile: Path):
    subs_to_nodes = collections.defaultdict(set)
    nodes_to_subs = {}
    raw_edges = []

    for line in Path(dagfile).open(mode="r"):
        job_match = EXTRACT_NODES.match(line)
        if job_match is not None:
            subs_to_nodes[job_match.group("file")].add(job_match.group("name"))
            nodes_to_subs[job_match.group("name")] = job_match.group("file")
            continue

        edges_match = EXTRACT_EDGES.match(line)
        if edges_match is not None:
            raw_edges.append(
                (
                    edges_match.group("parents").split(),
                    edges_match.group("children").split(),
                )
            )
            continue

    # print(raw_nodes)
    # print(raw_edges)

    assert sum(len(v) for k, v in subs_to_nodes.items()) == len(nodes_to_subs)

    return subs_to_nodes, nodes_to_subs, raw_edges


def merge_edges(subs_to_nodes, nodes_to_subs, raw_edges):
    edges = set()
    for parents, children in raw_edges:
        for parent, child in itertools.product(parents, children):
            edges.add((nodes_to_subs[parent], nodes_to_subs[child]))

    return edges


def dot(subs_to_nodes, nodes_to_subs, edges):
    g = Digraph(
        "dag",
        graph_attr={"dpi": str(600)},
        node_attr={"fontname": "Courier New", "shape": "box"},
        edge_attr={"fontname": "Courier New"},
    )
    g.format = "png"

    for sub, nodes in subs_to_nodes.items():
        g.node(sub, label="{}\n{}".format(sub, len(nodes)))

    for parent, child in edges:
        g.edge(parent, child)

    return g


if __name__ == "__main__":
    dagfile = Path(sys.argv[1])
    subs_to_nodes, nodes_to_subs, raw_edges = get_nodes_and_edges(dagfile)

    edges = merge_edges(subs_to_nodes, nodes_to_subs, raw_edges)

    g = dot(subs_to_nodes, nodes_to_subs, edges)
    g.render(dagfile.stem)
