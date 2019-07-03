#!/usr/bin/env python3

from pathlib import Path

import dagger
import htcondor

dag = dagger.DAG(
    jobstate_log="jobstate.log",
    config_file="dag.config",
    dot_config=dagger.DotConfig(
        "dag.dot", update=True, overwrite=False, include_file="include.dot"
    ),
    node_status_file=dagger.NodeStatusFile(
        "node_status_file", update_time=50, always_update=True
    ),
)

A = dag.node(
    name="A",
    postfix_format="{:0>4}",
    pre=dagger.Script(
        executable="/bin/echo",
        arguments=["foobar"],
        retry=True,
        retry_status=1,
        retry_delay=10,
    ),
    vars=[{"path": '" and \\'}],
)

# todo: what if I actually WANT a dense connections between two nodes with same number of vars?
B = A.child(name="B", vars=[{"target": idx} for idx in range(5)], noop=True)
C = B.child(name="C", vars=[{"target": idx} for idx in range(5)], done=True)

D = C.child(
    name="D",
    category="cat",
    priority=5,
    abort=dagger.DAGAbortCondition(node_exit_value=2, dag_return_value=5),
    pre_skip_exit_code=3,
)

for node in dag.nodes:
    print(node.name, node.parents, node.children)

writer = dagger.DAGWriter(dag)
writer.write(Path.cwd() / "out")
