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

from typing import Optional, Dict, Iterable, Union, List, Any
import logging

import os
import collections
import itertools
import functools
import enum
from pathlib import Path
import collections.abc
import fnmatch
import abc

import htcondor

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

    def __contains__(self, node):
        return node in self._nodes

    def node(self, **kwargs):
        node = NodeSet(dag=self, **kwargs)
        self.nodes.add(node)
        return node

    def subdag(self, **kwargs):
        node = SubDag(dag=self, **kwargs)
        self.nodes.add(node)
        return node

    def select(self, pattern):
        return Nodes(
            *(
                node
                for name, node in self._nodes.items()
                if fnmatch.fnmatchcase(name, pattern)
            )
        )

    def roots(self):
        return Nodes(*(node for node in self.nodes if len(node.parents) == 0))

    def walk(self, order: WalkOrder = WalkOrder.DEPTH_FIRST):
        seen = set()
        stack = collections.deque(self.roots())

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
    def __init__(self, path, update_time=None, always_update=False):
        self.path = path
        self.update_time = update_time
        self.always_update = always_update

    def __repr__(self):
        return f"{self.__class__.__name__}(path = {self.path}, update_time = {self.update_time})"


class DAGAbortCondition:
    def __init__(self, node_exit_value, dag_return_value=None):
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
        return f"{self.__class__.__name__}(executable = {self.executable}, arguments = {self.arguments}, retry = {self.retry}, retry_status = {self.retry_status}, retry_delay = {self.retry_delay})"


class NodeStore:
    def __init__(self):
        self.nodes = {}

    def add(self, *nodes):
        for node in nodes:
            if isinstance(node, BaseNode):
                self.nodes[node.name] = node
            elif isinstance(node, Nodes):
                self.nodes.update({n.name: n for n in node})
            else:
                raise TypeError(f"{node} is not a Node or a Nodes")

    def remove(self, *nodes):
        for node in nodes:
            if isinstance(node, str):
                self.nodes.pop(node, None)
            elif isinstance(node, BaseNode):
                self.nodes.pop(node.name, None)
            elif isinstance(node, Nodes):
                for n in node:
                    self.nodes.pop(n.name, None)

    def __getitem__(self, node):
        if isinstance(node, str):
            return self.nodes[node]
        elif isinstance(node, NodeSet):
            return self.nodes[node.name]
        else:
            raise KeyError()

    def __iter__(self):
        yield from self.nodes.values()

    def __contains__(self, node):
        if isinstance(node, NodeSet):
            return node in self.nodes.values()
        elif isinstance(node, str):
            return node in self.nodes.keys()
        return False

    def items(self):
        yield from self.nodes.items()

    def __repr__(self):
        return repr(set(self.nodes.values()))

    def __str__(self):
        return str(set(self.nodes.values()))

    def __len__(self):
        return len(self.nodes)


def flatten(nested_iterable) -> List[Any]:
    return list(itertools.chain.from_iterable(nested_iterable))


@functools.total_ordering
class BaseNode(abc.ABC):
    def __init__(
        self,
        dag,
        *,
        name: str,
        dir: Optional[os.PathLike] = None,
        noop: bool = False,
        done: bool = False,
        retries: Optional[int] = 0,
        retry_unless_exit: Optional[int] = None,
        pre: Optional[Script] = None,
        pre_skip_exit_code=None,
        post: Optional[Script] = None,
        priority: int = 0,
        category: Optional[str] = None,
        abort: Optional[DAGAbortCondition] = None,
    ):
        self._dag = dag
        self.name = name

        self.parents = NodeStore()
        self.children = NodeStore()

        self.dir = Path(dir) if dir is not None else None
        self.noop = noop
        self.done = done

        self.retries = retries
        self.retry_unless_exit = retry_unless_exit
        self.priority = priority
        self.category = category
        self.abort = abort

        self.pre = pre
        self.pre_skip_exit_code = pre_skip_exit_code
        self.post = post

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    def description(self):
        data = "\n".join(f"  {k} = {v}" for k, v in self.__dict__.items())
        return f"{self.__class__.__name__}(\n{data}\n)"

    def __iter__(self):
        yield self

    def __hash__(self):
        return hash((self.__class__, self.name))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, NodeSet):
            return NotImplemented
        return self.name < other.name

    def child(self, **kwargs):
        node = self._dag.node(**kwargs)

        node.parents.add(self)
        self.children.add(node)

        return node

    def parent(self, **kwargs):
        node = self._dag.node(**kwargs)

        node.children.add(self)
        self.parents.add(node)

        return node

    def add_children(self, *nodes):
        nodes = flatten(nodes)
        self.children.add(*nodes)
        for node in nodes:
            node.parents.add(self)

    def remove_children(self, *nodes):
        nodes = flatten(nodes)
        self.children.remove(*nodes)
        for node in nodes:
            node.parents.remove(self)

    def add_parents(self, *nodes):
        nodes = flatten(nodes)
        self.parents.add(*nodes)
        for node in nodes:
            node.children.add(self)

    def remove_parents(self, *nodes):
        nodes = flatten(nodes)
        self.parents.remove(*nodes)
        for node in nodes:
            node.children.remove(self)


class NodeSet(BaseNode):
    def __init__(
        self,
        dag: DAG,
        *,
        postfix_format="{:d}",
        submit_description: Optional[htcondor.Submit] = None,
        vars: Optional[Iterable[Dict[str, str]]] = None,
        **kwargs,
    ):
        super().__init__(dag, **kwargs)

        self.postfix_format = postfix_format

        self.submit_description = submit_description or htcondor.Submit({})

        if vars is None:
            vars = [{}]
        self.vars = list(vars)


class SubDag(BaseNode):
    def __init__(self, dag: DAG, *, dag_file: Path, **kwargs):
        super().__init__(dag, **kwargs)

        self.dag_file = dag_file


class Nodes:
    def __init__(self, *nodes):
        self.nodes = NodeStore()
        nodes = flatten(nodes)
        for node in nodes:
            self.nodes.add(node)

    def __iter__(self):
        yield from self.nodes

    def __contains__(self, node):
        return node in self.nodes

    def __repr__(self):
        return f"Nodes({', '.join(repr(n) for n in sorted(self.nodes, key = lambda n: n.name))})"

    def __str__(self):
        return f"Nodes({', '.join(str(n) for n in sorted(self.nodes, key = lambda n: n.name))})"

    def _some_element(self):
        return next(iter(self.nodes))

    def child(self, **kwargs):
        node = self._some_element().child(**kwargs)

        for s in self:
            node.parents.add(s)
            s.children.add(node)

        return node

    def parent(self, **kwargs):
        node = self._some_element().child(**kwargs)

        for s in self:
            node.children.add(self)
            s.parents.add(node)

        return node

    def add_children(self, *nodes):
        for s in self:
            s.children.add(*nodes)
            for node in nodes:
                node.parents.add(s)

    def remove_children(self, *nodes):
        for s in self:
            s.children.remove(*nodes)
            for node in nodes:
                node.parents.remove(s)

    def add_parents(self, *nodes):
        for s in self:
            s.parents.add(*nodes)
            for node in nodes:
                node.children.add(s)

    def remove_parents(self, *nodes):
        for s in self:
            s.parents.remove(*nodes)
            for node in nodes:
                node.children.remove(s)
