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
    raw_nodes = []
    raw_edges = []

    for line in Path(dagfile).open(mode="r"):
        job_match = EXTRACT_NODES.match(line)
        if job_match is not None:
            raw_nodes.append(job_match.group("name"))
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

    return raw_nodes, raw_edges


def nodes_to_layers(nodes, prefixes):
    layer_to_width = collections.Counter()
    for node in nodes:
        prefix = max(p for p in prefixes if node.startswith(p))
        layer_to_width[prefix] += 1

    if sum(layer_to_width.values()) != len(nodes):
        missing = "\n".join(
            n for n in nodes if not any(n.startswith(prefix) for prefix in prefixes)
        )
        raise Exception(f"Missing prefixes! Remaining nodes:\n{missing}")

    return layer_to_width


def compactify_nodes_and_edges(raw_nodes, raw_edges, prefixes):
    layers = nodes_to_layers(raw_nodes, prefixes)

    edges = set()
    for parents, children in raw_edges:
        parents = tuple(nodes_to_layers(parents, prefixes).keys())
        children = tuple(nodes_to_layers(children, prefixes).keys())
        edges.add((parents, children))

    return layers, edges


def dot(layers, edges):
    g = Digraph(
        "dag",
        graph_attr={"dpi": str(600)},
        node_attr={"fontname": "Courier New", "shape": "box"},
        edge_attr={"fontname": "Courier New"},
    )
    g.format = "png"

    for layer, width in layers.items():
        g.node(layer, label="{}\n{}".format(layer, width))

    for parents, children in edges:
        for parent, child in itertools.product(parents, children):
            g.edge(parent, child)

    return g


if __name__ == "__main__":
    dagfile = Path(sys.argv[1])
    raw_nodes, raw_edges = get_nodes_and_edges(dagfile)

    prefixes = [
        "ligolw_add",
        "gstlal_injsplitter",
        "gstlal_plot_psd_horizon",
        "gstlal_reference_psd",
        "gstlal_median_of_psds",
        "ligolw_sqlite_from_xml",
        "gstlal_svd_bank",
        "gstlal_inspiral_inj",
        "gstlal_inspiral_calc_likelihood_inj",
        "gstlal_inspiral_calc_likelihood",
        "gstlal_inspiral_marginalize_likelihood_with_zerolag",
        "gstlal_inspiral_marginalize_likelihood",
        "gstlal_inspiral_plotsummary_isolated_precession_inj",
        "gstlal_inspiral_plotsummary_isolated_precession",
        "gstlal_inspiral_plotsummary_inj",
        "gstlal_inspiral",
        "rm_intermediate_merger_products",
        "lalapps_inspinjfind",
        "lalapps_run_sqlite",
        "gstlal_inspiral_calc_rank_pdfs",
        "gstlal_inspiral_create_prior_diststats",
        "gstlal_compute_far_from_snr_chisq_histograms",
        "gstlal_inspiral_summary_page",
        "gstlal_inspiral_plot_background",
        "gstlal_inspiral_plot_sensitivity",
        "gstlal_inspiral_plotsummary",
        "ligolw_sqlite_to_xml",
        "cp",
    ]

    layers, edges = compactify_nodes_and_edges(raw_nodes, raw_edges, prefixes)

    g = dot(layers, edges)
    g.render(dagfile.stem)
