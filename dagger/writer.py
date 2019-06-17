# Copyright 2019 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional, MutableMapping, List, Dict, Iterable, Union
import logging

import os
import collections
import enum
import itertools
from pathlib import Path
import re
import sys
import time
import heapq
import subprocess
import collections.abc
import fnmatch

import htcondor
import htcondor_jobs as jobs

from .dag import WalkOrder

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SEPARATOR = ":"


class DAGWriter:
    def __init__(self, dag, path):
        self.dag = dag
        self.path = path
        self.join_counter = None

    def write(self):
        self.path.mkdir(parents=True, exist_ok=True)
        self.join_counter = itertools.count()

        with (self.path / "dagfile.dag").open(mode="w") as f:
            f.write("# BEGIN CONFIG\n")
            for line in itertools.chain(self._get_dag_config_lines()):
                f.write(line)
                f.write("\n")

            f.write("# END CONFIG\n")
            f.write("# BEGIN NODES AND EDGES\n")
            for node in self.dag.walk(order=WalkOrder.BREADTH_FIRST):
                self.write_submit_file(node)

                for line in itertools.chain(
                    self._get_node_lines(node), self._get_edge_lines(node)
                ):
                    f.write(line)
                    f.write("\n")
            f.write("# END NODES AND EDGES\n")

    def _get_dag_config_lines(self):
        if self.dag.config_file is not None:
            yield f"CONFIG {self.dag.config_file}"

        if self.dag.jobstate_log is not None:
            yield f"JOBSTATE_LOG {self.dag.jobstate_log}"

        if self.dag.node_status_file is not None:
            nsf = self.dag.node_status_file
            parts = ["NODE_STATUS_FILE", nsf.path]
            if nsf.update_time is not None:
                parts.append(str(nsf.update_time))
            if nsf.always_update:
                parts.append("ALWAYS-UPDATE")
            yield " ".join(parts)

        if self.dag.dot_config is not None:
            c = self.dag.dot_config
            parts = [
                "DOT",
                c.path,
                "UPDATE" if c.update else "DONT-UPDATE",
                "OVERWRITE" if c.overwrite else "DONT-OVERWRITE",
            ]
            if c.include_file is not None:
                parts.extend(("INCLUDE", c.include_file))
            yield " ".join(parts)

        for category, value in self.dag.max_jobs_per_category:
            yield f"CATEGORY {category} {value}"

    def write_submit_file(self, node):
        (self.path / f"{node.name}.sub").write_text(str(node.submit_description))

    def _get_node_lines(self, node):
        for idx, v in enumerate(node.vars):
            name = f"{node.name}{SEPARATOR}{node.postfix_format.format(idx)}"
            yield f"JOB {name}"

            if len(v) > 0:
                parts = [f"VARS {name}"]
                for key, value in v.items():
                    value_text = str(value).replace("\\", "\\\\").replace('"', r"\"")
                    parts.append(f'{key} = "{value_text}"')
                yield " ".join(parts)

            if node.pre is not None:
                yield from self._get_script_line(name, node.pre, "PRE")
            if node.post is not None:
                yield from self._get_script_line(name, node.post, "POST")

    def _get_script_line(self, name, script, which):
        parts = ["SCRIPT"]

        if script.retry:
            parts.append("DEFER")
            parts.append(script.retry_status)
            parts.append(script.retry_delay)

        parts.append(which.upper())
        parts.append(name)
        parts.append(script.executable)
        parts.extend(script.arguments)

        yield " ".join(str(p) for p in parts)

    def _get_edge_lines(self, node):
        for child in node.children:
            parents = (
                f"{node.name}{SEPARATOR}{node.postfix_format.format(idx)}"
                for idx in range(len(node.vars))
            )
            children = (
                f"{child.name}{SEPARATOR}{child.postfix_format.format(idx)}"
                for idx in range(len(child.vars))
            )

            if len(node.vars) == len(child.vars):
                for p, c in zip(parents, children):
                    yield f"PARENT {p} CHILD {c}"
            elif len(node.vars) == 1 or len(child.vars) == 1:
                yield f"PARENT {' '.join(parents)} CHILD {' '.join(children)}"
            else:
                join_name = f"__JOIN~{next(self.join_counter)}__"
                noop_sub_name = "__JOIN__.sub"
                (self.path / noop_sub_name).touch(exist_ok=True)
                yield f"JOB {join_name} {noop_sub_name} NOOP"
                yield f"PARENT {' '.join(parents)} CHILD {join_name}"
                yield f"PARENT {join_name} CHILD {' '.join(children)}"
