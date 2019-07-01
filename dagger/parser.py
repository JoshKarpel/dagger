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

from typing import Optional, MutableMapping, List
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

import htcondor
import htcondor_jobs as jobs

from . import dag

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class RawNodeDict(collections.defaultdict):
    def __missing__(self, key):
        self[key] = RawNode(name=key)
        return self[key]


CMD_REGEXES = dict(
    dot=re.compile(r"^DOT\s+(?P<filename>\S+)(\s+(?P<options>.+))?", re.IGNORECASE),
    job=re.compile(
        r"^JOB\s+(?P<name>\S+)\s+(?P<filename>\S+)(\s+DIR\s+(?P<directory>\S+))?(\s+(?P<noop>NOOP))?(\s+(?P<done>DONE))?",
        re.IGNORECASE,
    ),
    priority=re.compile(r"^PRIORITY\s+(?P<name>\S+)\s+(?P<value>\S+)", re.IGNORECASE),
    category=re.compile(
        r"^CATEGORY\s+(?P<name>\S+)\s+(?P<category>\S+)", re.IGNORECASE
    ),
    retry=re.compile(
        r"^RETRY\s+(?P<name>\S+)\s+(?P<retries>\S+)(\s+UNLESS-EXIT\s+(?P<retry_unless_exit_value>\S+))?",
        re.IGNORECASE,
    ),
    vars=re.compile(r"^VARS\s+(?P<name>\S+)\s+(?P<vars>.+)", re.IGNORECASE),
    script=re.compile(
        r"^SCRIPT\s+(DEFER\s+(?P<defer_status>\d+)\s+(?P<defer_delay>\d+)\s+)?(?P<type>(PRE)|(POST))\s+(?P<name>\S+)\s+(?P<executable>\S+)(\s+(?P<arguments>.+))?",
        re.IGNORECASE,
    ),
    abort_dag_on=re.compile(
        r"^ABORT-DAG-ON\s+(?P<name>\S+)\s+(?P<exitvalue>\S+)(\s+RETURN\s+(?P<returnvalue>\S+))?",
        re.IGNORECASE,
    ),
    edges=re.compile(
        r"^PARENT\s+(?P<parents>.+?)\s+CHILD\s+(?P<children>.+)", re.IGNORECASE
    ),
    maxjobs=re.compile(r"^MAXJOBS\s+(?P<category>\S+)\s+(?P<value>\S+)", re.IGNORECASE),
    config=re.compile(r"^CONFIG\s+(?P<filename>\S+)", re.IGNORECASE),
    include=re.compile(r"^INCLUDE\s+(?P<filename>\S+)", re.IGNORECASE),
    node_status_file=re.compile(
        r"^NODE_STATUS_FILE\s+(?P<filename>\S+)(\s+(?P<updatetime>\S+))?", re.IGNORECASE
    ),
    jobstate_log=re.compile(r"^JOBSTATE_LOG\s+(?P<filename>\S+)", re.IGNORECASE),
    # subdag=re.compile(
    #     r"^SUBDAG\s+EXTERNAL\s+(?P<name>\S+)\s+(?P<filename>\S+)(\s+DIR\s+(?P<directory>\S+))?(\s+(?P<noop>NOOP))?(\s+(?P<done>DONE))?",
    #     re.IGNORECASE,
    # ),
    # splice=re.compile(
    #     r"^SPLICE\s+(?P<name>\S+)\s+(?P<filename>\S+)(\s+DIR\s+(?P<directory>\S+))?",
    #     re.IGNORECASE,
    # ),
)

EXTRACT_VARS = re.compile(r'(?P<key>\S+)\s*=\s*"(?P<value>.*?)(?<!\\)"', re.IGNORECASE)


class DAGCommandParser:
    def __init__(self):
        self.nodes: MutableMapping[str, RawNode] = RawNodeDict()
        self.jobstate_log = None
        self.max_jobs_by_category = {}
        self.config_file = None
        self.dot_config = None
        self.node_status_file = None

    def to_dag(self) -> dag.DAG:
        d = dag.DAG(
            jobstate_log=self.jobstate_log,
            max_jobs_by_category=self.max_jobs_by_category.copy(),
            config_file=self.config_file,
            dot_config=self.dot_config,
            node_status_file=self.node_status_file,
        )

        for node in self.nodes.values():
            d.node(**node.to_node_kwargs())

        for name, node in self.nodes.items():
            d.nodes[name].children.add(*(d.nodes[n.name] for n in node.children))
            d.nodes[name].parents.add(*(d.nodes[n.name] for n in node.parents))

        return d

    def parse_file(self, path: os.PathLike):
        path = Path(path)

        with path.open(mode="r") as f:
            for line_number, line in enumerate(f, start=1):
                self.parse_line(line, line_number)

        return self

    def parse_line(self, line, line_number=0):
        line = line.strip()

        if line.startswith("#"):
            return self

        if line == "":
            return self

        for cmd, pattern in CMD_REGEXES.items():
            match = pattern.search(line)
            if match is not None:
                getattr(self, f"_process_{cmd}")(match, line_number)
                return self
        else:
            raise Exception(f"unrecognized command on line {line_number}: {line}")

    def _process_job(self, match, line_number):
        node = self.nodes[match.group("name")]

        node.submit_file = Path(match.group("filename"))
        dir = match.group("directory")
        node.dir = Path(dir) if dir is not None else None
        node.done = bool(match.group("done"))
        node.noop = bool(match.group("noop"))

    def _process_retry(self, match, line_number):
        node = self.nodes[match.group("name")]

        node.retries = match.group("retries")
        node.retry_unless_exit_value = match.group("retry_unless_exit_value")

    def _process_edges(self, match, line_number):
        parents = match.group("parents").strip().split()
        children = match.group("children").strip().split()

        for parent, child in itertools.product(parents, children):
            parent_node, child_node = self.nodes[parent], self.nodes[child]
            parent_node.children.add(child_node)
            child_node.parents.add(parent_node)

    def _process_vars(self, match, line_number):
        node = self.nodes[match.group("name")]

        for name, value in EXTRACT_VARS.findall(match.group("vars")):
            if name in node.vars:
                raise Exception(f"duplicate vars on line {line_number}")
            # apply unescape rules to the value
            node.vars[name] = value.replace("\\\\", "\\").replace('\\"', '"')

    def _process_priority(self, match, line_number):
        node = self.nodes[match.group("name")]
        node.priority = int(match.group("value"))

    def _process_jobstate_log(self, match, line_number):
        if self.jobstate_log is None:
            self.jobstate_log = match.group("filename")

    def _process_script(self, match, line_number):
        node = self.nodes[match.group("name")]

        type = match.group("type").lower()
        args = match.group("arguments")

        setattr(
            node,
            type,
            dag.Script(
                executable=match.group("executable"),
                arguments=args.split() if args is not None else None,
                retry=True if match.group("defer_status") is not None else False,
                retry_status=match.group("defer_status"),
                retry_delay=match.group("defer_delay"),
            ),
        )

    def _process_abort_dag_on(self, match, line_number):
        node = self.nodes[match.group("name")]

        node.abort = dag.DAGAbortCondition(
            node_exit_value=match.group("exitvalue"),
            dag_return_value=match.group("returnvalue"),
        )

    def _process_category(self, match, line_number):
        node = self.nodes[match.group("name")]

        node.category = match.group("category")

    def _process_maxjobs(self, match, line_number):
        self.max_jobs_by_category[match.group("category")] = match.group("value")

    def _process_config(self, match, line_number):
        self.config_file = match.group("filename")

    def _process_dot(self, match, line_number):
        path = match.group("filename")

        options = (match.group("options") or "").split()
        kwargs = {}
        while len(options) > 0:
            option = options.pop(0).upper()
            if option == "UPDATE":
                kwargs["update"] = True
            elif option == "DONT-UPDATE":
                kwargs["update"] = False
            elif option == "OVERWRITE":
                kwargs["overwrite"] = True
            elif option == "DONT-OVERWRITE":
                kwargs["overwrite"] = False
            elif option == "INCLUDE":
                try:
                    kwargs["include_file"] = options.pop(0)
                except IndexError:
                    raise Exception(
                        f"line {line_number}: missing filename for INCLUDE option of DOT"
                    )
            else:
                raise Exception(f"unrecognized option {option} for DOT")

            self.dot_config = dag.DotConfig(path, **kwargs)

    def _process_node_status_file(self, match, line_number):
        path = match.group("filename")
        update_time = match.group("updatetime")
        if update_time != "ALWAYS-UPDATE":
            update_time = int(match.group("updatetime"))

        self.node_status_file = dag.NodeStatusFile(path=path, update_time=update_time)

    def _process_include(self, match, line_number):
        path = match.group("filename")

        self.parse_file(Path(path))


class RawNode:
    def __init__(
        self,
        name,
        submit_file=None,
        dir=None,
        noop=False,
        done=False,
        vars=None,
        retries=0,
        retry_unless_exit=None,
        pre=None,
        post=None,
        parents=None,
        children=None,
        priority=0,
        category=None,
        abort=None,
    ):
        self.name = name
        self.submit_file = Path(submit_file) if submit_file is not None else None
        self.dir = Path(dir) if dir is not None else None
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

        self.parents = parents or set()
        self.children = children or set()

    def to_node_kwargs(self) -> dict:
        return dict(
            name=self.name,
            submit_description=htcondor.Submit(self.submit_file.read_text()),
            dir=self.dir,
            noop=self.noop,
            done=self.done,
            pre=self.pre,
            post=self.post,
        )

    @property
    def submit_file(self):
        return self._submit_file

    @submit_file.setter
    def submit_file(self, path: Optional[Path]):
        self._submit_file = Path(path) if path is not None else None

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    def description(self):
        data = "\n".join(f"  {k} = {v}" for k, v in self.__dict__.items())
        return f"{self.__class__.__name__}(\n{data}\n)"

    def __hash__(self):
        return hash((self.__class__, self.name))

    def __eq__(self, other):
        return isinstance(other, RawNode) and self.name == other.name
