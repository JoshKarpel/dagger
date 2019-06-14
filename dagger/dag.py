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

from typing import Optional, MutableMapping, List, Dict, Iterable
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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class WalkOrder(enum.Enum):
    DEPTH_FIRST = "DEPTH"
    BREADTH_FIRST = "BREADTH"


class DAG:
    def __init__(
        self,
        jobstate_log=None,
        max_jobs_by_category=None,
        config_file=None,
        dot_config=None,
        node_status_file=None,
    ):
        self._nodes = NodeStore()
        self.jobstate_log = jobstate_log
        self.max_jobs_per_category = max_jobs_by_category or {}
        self.config_file = config_file
        self.dot_config = dot_config
        self.node_status_file = node_status_file

    @property
    def nodes(self):
        return self._nodes

    def node(self, **kwargs):
        node = Node(dag=self, **kwargs)
        self.nodes.add(node)
        return node

    def select(self, pattern):
        return Nodes(
            node
            for name, node in self._nodes.items()
            if fnmatch.fnmatchcase(name, pattern)
        )

    def walk(self, order: WalkOrder = WalkOrder.DEPTH_FIRST):
        seen = set()
        stack = collections.deque(
            sorted(
                (node for node in self.nodes if len(node.parents) == 0),
                key=lambda node: node.name,
            )
        )

        while len(stack) != 0:
            if order is WalkOrder.DEPTH_FIRST:
                node = stack.pop()
            elif order is WalkOrder.BREADTH_FIRST:
                node = stack.popleft()
            else:
                raise Exception("Unrecognized WalkOrder")

            if node in seen:
                continue
            seen.add(node)

            stack.extend(node.children)
            yield node


class NodeStatusFile:
    def __init__(self, path, update_time=None):
        self.path = path
        self.update_time = update_time

    def __repr__(self):
        return f"{self.__class__.__name__}(path = {self.path}, update_time = {self.update_time})"


class DAGAbortCondition:
    def __init__(self, node_exit_value, dag_return_value):
        self.node_exit_value = node_exit_value
        self.dag_return_value = dag_return_value

    def __repr__(self):
        return f"{self.__class__.__name__}(node_exit_value = {self.node_exit_value}, dag_return_value = {self.dag_return_value})"


class DotConfig:
    def __init__(self, path, update=False, overwrite=True, include_file=None):
        self.path = path
        self.update = update
        self.overwrite = overwrite
        self.include_file = include_file

    def __repr__(self):
        return f"{self.__class__.__name__}(update = {self.update}, overwrite = {self.overwrite}, include = {self.include_file})"


class Script:
    def __init__(
        self, executable, arguments=None, retry=False, retry_status=1, retry_delay=0
    ):
        self.executable = executable
        self.retry = retry
        self.retry_status = retry_status
        self.retry_delay = retry_delay

        if arguments is None:
            arguments = []
        self.arguments = arguments

    def __repr__(self):
        return f"{self.__class__.__name__}(executable = {self.executable}, arguments = {self.arguments}, retry_status = {self.retry_status}, retry_delay = {self.retry_delay})"


class NodeStore:
    def __init__(self):
        self.nodes = {}

    def add(self, *nodes):
        for node in nodes:
            self.nodes[node.name] = node

    def remove(self, *nodes):
        for node in nodes:
            if isinstance(node, str):
                self.nodes.pop(node, None)
            elif isinstance(node, Node):
                self.nodes.pop(node.name, None)

    def __getitem__(self, node):
        if isinstance(node, str):
            return self.nodes[node]
        elif isinstance(node, Node):
            return self.nodes[node.name]
        else:
            raise KeyError()

    def __iter__(self):
        yield from self.nodes.values()

    def items(self):
        yield from self.nodes.items()

    def __repr__(self):
        return repr(set(self.nodes.values()))

    def __str__(self):
        return str(set(self.nodes.values()))

    def __len__(self):
        return len(self.nodes)


class Node:
    def __init__(
        self,
        dag: DAG,
        *,
        name: str,
        submit_description: Optional[htcondor.Submit] = None,
        dir: Optional[os.PathLike] = None,
        noop: bool = False,
        done: bool = False,
        vars: Optional[Dict[str, str]] = None,
        retries: Optional[int] = 0,
        retry_unless_exit: Optional[int] = None,
        pre: Optional[Script] = None,
        post: Optional[Script] = None,
        priority: int = 0,
        category: Optional[str] = None,
        abort: Optional[DAGAbortCondition] = None,
    ):
        self._dag = dag

        self.name = name
        self.submit_description = submit_description or htcondor.Submit({})
        self.dir = Path(dir) if dir is not None else dir
        self.noop = noop
        self.done = done
        self.vars = vars or {}
        self.retries = retries
        self.retry_unless_exit = retry_unless_exit
        self.priority = priority
        self.category = category
        self.abort = abort

        self.pre = pre
        self.post = post

        self.parents = NodeStore()
        self.children = NodeStore()

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    def description(self):
        data = "\n".join(f"  {k} = {v}" for k, v in self.__dict__.items())
        return f"{self.__class__.__name__}(\n{data}\n)"

    def __hash__(self):
        return hash((self.__class__, self.name))

    def __eq__(self, other):
        return isinstance(other, Node) and self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, Node):
            raise TypeError(
                f"{self.__class__.__name__} does not support < with {other.__class__.__name__}"
            )
        return self.name < other.name

    def child(self, **kwargs):
        node = self._dag.node(**kwargs)

        node.parents.add(self)
        self.children.add(node)

        return node


class Nodes:
    def __init__(self, nodes):
        self._nodes = set(nodes)
        self._dag = next(iter(self._nodes))._dag

    def child(self, **kwargs):
        node = self._dag.node(**kwargs)

        node.parents.add(*self._nodes)
        for parent in self:
            parent.children.add(node)

        return node

    def __iter__(self):
        yield from self._nodes
