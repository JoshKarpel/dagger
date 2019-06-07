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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class NodeDict(collections.defaultdict):
    def __missing__(self, key):
        self[key] = Node(name=key)
        return self[key]


CMD_REGEXES = dict(
    dot=re.compile(r"^DOT\s+(?P<filename>\S+)(\s+(?P<options>.+))?", re.IGNORECASE),
    job=re.compile(
        r"^JOB\s+(?P<name>\S+)\s+(?P<filename>\S+)(\s+DIR\s+(?P<directory>\S+))?(\s+(?P<noop>NOOP))?(\s+(?P<done>DONE))?",
        re.IGNORECASE,
    ),
    data=re.compile(
        r"^DATA\s+(?P<name>\S+)\s+(?P<filename>\S+)(\s+DIR\s+(?P<directory>\S+))?(\s+(?P<noop>NOOP))?(\s+(?P<done>DONE))?",
        re.IGNORECASE,
    ),
    subdag=re.compile(
        r"^SUBDAG\s+EXTERNAL\s+(?P<name>\S+)\s+(?P<filename>\S+)(\s+DIR\s+(?P<directory>\S+))?(\s+(?P<noop>NOOP))?(\s+(?P<done>DONE))?",
        re.IGNORECASE,
    ),
    splice=re.compile(
        r"^SPLICE\s+(?P<name>\S+)\s+(?P<filename>\S+)(\s+DIR\s+(?P<directory>\S+))?",
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
        r"^SCRIPT\s+(?P<type>(PRE)|(POST))\s(?P<name>\S+)\s+(?P<executable>\S+)(\s+(?P<arguments>.+))?",
        re.IGNORECASE,
    ),
    abortdagon=re.compile(
        r"^ABORT-DAG-ON\s+(?P<name>\S+)\s+(?P<exitvalue>\S+)(\s+RETURN\s+(?P<returnvalue>\S+))?",
        re.IGNORECASE,
    ),
    edges=re.compile(
        r"^PARENT\s+(?P<parents>.+?)\s+CHILD\s+(?P<children>.+)", re.IGNORECASE
    ),
    maxjobs=re.compile(r"^MAXJOBS\s+(?P<category>\S+)\s+(?P<value>\S+)", re.IGNORECASE),
    config=re.compile(r"^CONFIG\s+(?P<filename>\S+)", re.IGNORECASE),
    nodestatus=re.compile(
        r"^NODE_STATUS_FILE\s+(?P<filename>\S+)(\s+(?P<updatetime>\S+))?", re.IGNORECASE
    ),
    jobstate=re.compile(r"^JOBSTATE_LOG\s+(?P<filename>\S+)", re.IGNORECASE),
)

EXTRACT_VARS = re.compile(r'(?P<key>\S+)\s*=\s*"(?P<value>.*?)(?<!\\)"', re.IGNORECASE)


class DAG:
    def __init__(self):
        self.nodes = NodeDict()
        self.jobstate_log = None

    @classmethod
    def from_file(cls, path: Path):
        path = Path(path)
        dag = cls()

        with path.open(mode="r") as f:
            for line_number, line in enumerate(f, start=1):
                dag._process_line(line, line_number)

        return dag

    def _process_line(self, line, line_number):
        if line.startswith("#"):
            return

        for cmd, pattern in CMD_REGEXES.items():
            match = pattern.search(line)
            if match is not None:
                getattr(self, f"_process_{cmd}", lambda m, l: print(cmd, l))(
                    match, line_number
                )
                break

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

    def _process_jobstate(self, match, line_number):
        if self.jobstate_log is None:
            self.jobstate_log = match.group("filename")

    def _process_script(self, match, line_number):
        node = self.nodes[match.group("name")]

        type = ScriptType(match.group("type").upper())

        args = match.group("arguments")
        node.scripts[type] = Script(
            node=node,
            type=type,
            executable=match.group("executable"),
            arguments=args.split() if args is not None else None,
        )


class Node:
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
        parents=None,
        children=None,
        priority=0,
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

        self.scripts = {}

        self.parents = parents or set()
        self.children = children or set()

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
        return self.name < other.name


class ScriptType(str, enum.Enum):
    PRE = "PRE"
    POST = "POST"


class Script:
    def __init__(
        self,
        node,
        type,
        executable,
        arguments=None,
        retry=False,
        retry_status=1,
        retry_delay=0,
    ):
        self.node = node
        self.type = type
        self.executable = executable
        self.retry = retry
        self.retry_status = retry_status
        self.retry_delay = retry_delay

        if arguments is None:
            arguments = []
        self.arguments = arguments

    def __str__(self):
        parts = ["SCRIPT"]
        if self.retry:
            parts.extend(["DEFER", str(self.retry_status), str(self.retry_delay)])
        parts.extend(
            [self.type, self.node.name, self.executable, " ".join(self.arguments)]
        )
        return " ".join(parts)


class Executor:
    def __init__(self, dag, max_execute_per_cycle=None, min_loop_delay=1):
        self.dag = dag
        self.max_execute_per_cycle = max_execute_per_cycle
        self.min_loop_delay = min_loop_delay

    def execute(self):
        waiting_nodes = set(self.dag.nodes.values())
        executable_nodes = []
        executing_nodes = set()

        remaining_parents = {n: n.parents.copy() for n in waiting_nodes}

        num_done = 0

        cycle_counter = itertools.count()

        while len(waiting_nodes) + len(executable_nodes) + len(executing_nodes) > 0:
            cycle_start = time.time()
            cycle = next(cycle_counter)

            logger.debug(f"beginning execute cycle {cycle}")
            logger.debug(f"waiting nodes: {waiting_nodes}")
            logger.debug(f"executable nodes: {executable_nodes}")
            logger.debug(f"executing nodes: {executing_nodes}")

            for node in waiting_nodes.copy():
                if len(remaining_parents[node]) == 0:
                    logger.debug(f"node {node.name} can execute")
                    heapq.heappush(executable_nodes, (node.priority, node))
                    waiting_nodes.remove(node)

            num_executed = 0
            while len(executable_nodes) > 0:
                if (
                    self.max_execute_per_cycle is not None
                    and num_executed > self.max_execute_per_cycle
                ):
                    logger.debug("broke because hit max_execute_per_cycle")
                    break

                prio, node = heapq.heappop(executable_nodes)
                logger.debug(f"considering node {node} with prio {prio} for execution")

                if node.noop:
                    logger.debug(f"node {node} was NOOP")
                    handle = None
                else:
                    self._run_script(node, ScriptType.PRE)
                    handle = self._run_node(node)

                executing_nodes.add((node, handle))
                num_executed += 1

            for node, handle in executing_nodes.copy():
                if self._is_node_complete(handle):
                    self._run_script(node, ScriptType.POST)
                    for child in node.children:
                        remaining_parents[child].remove(node)
                    executing_nodes.remove((node, handle))
                    num_done += 1
                    logger.debug(f"node {node} is complete")
                else:
                    logger.debug(f"node {node} is not complete")

            loop_time = time.time() - cycle_start
            sleep = max(self.min_loop_delay - loop_time, 0)

            logger.debug(
                f"finished execute cycle {cycle} in {loop_time:.6f} seconds, sleeping {sleep:.6f} seconds before next loop"
            )
            time.sleep(sleep)

        return num_done

    def _run_node(self, node: Node):
        sub = htcondor.Submit(node.submit_file.read_text())
        for k, v in node.vars.items():
            sub[k] = v

        sub["dag_node_name"] = node.name
        # sub['+DAGManJobId'] = # todo: get cluster
        # sub['DAGManJobId'] = # todo: same
        sub["submit_event_notes"] = f"DAG Node: {node.name}"
        # sub['dagman_log'] = # todo: where does this come from?
        # sub["+DAGManNodesMask"] = '"' # todo: getEventMask() produces this
        sub["priority"] = str(node.priority)
        # some conditional coming in to suppress node job logs
        sub["+DAGParentNodeNames"] = f"\"{' '.join(n.name for n in node.parents)}\""
        # something about DAG_STATUS
        # something about FAILED_COUNT
        # something about holding claims
        # something about suppressing notifications
        # something about accounting group and user

        logger.debug(f"submit description for node {node} is {sub}")

        currdir = os.getcwd()
        if node.dir is not None:
            os.chdir(node.dir)

        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            result = sub.queue_with_itemdata(txn)

        os.chdir(currdir)

        handle = jobs.ClusterHandle(result)
        logger.debug(f"handle is {handle}")

        return handle

    def _run_script(self, node: Node, which: ScriptType):
        try:
            script = node.scripts[which]
        except KeyError:
            logger.debug(f"no {which}script for node {node.name}")
            return

        logger.debug(f"running {which}script for node {node.name}")

        processed_args = []
        for arg in script.arguments:
            arg = arg.replace("$JOB", node.name)

            processed_args.append(arg)

        logger.debug(
            f'running subprocess: "{script.executable} {" ".join(processed_args)}"'
        )
        p = subprocess.run([script.executable, *processed_args], capture_output=True)
        logger.debug(f"subprocess result for node {node} {which}script: {p}")

    def _is_node_complete(self, handle):
        if handle is None:  # no handle means noop node
            return True
        return handle.state.is_complete()
