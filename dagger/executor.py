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


class Heap:
    def __init__(self, initial_data=None, key=None):
        if initial_data is None:
            initial_data = []
        if key is None:
            key = lambda item: item
        self.data = list(initial_data)
        self.key = key

    def push(self, item):
        heapq.heappush(self.data, (self.key(item), item))

    def pop(self):
        _, item = heapq.heappop(self.data)
        return item

    def __len__(self):
        return len(self.data)


class Executor:
    def __init__(self, dag, max_execute_per_cycle=None, min_loop_delay=1):
        self.dag = dag
        self.max_execute_per_cycle = max_execute_per_cycle
        self.min_loop_delay = min_loop_delay
        try:
            self.executor_cluster_id = os.environ["CONDOR_ID"].split(".")[0]
        except KeyError:
            self.executor_cluster_id = "-1"
        self.event_log_path = Path().cwd() / "dag_events.log"

        self.waiting_nodes = set(self.dag.nodes)
        self.executable_nodes = Heap(key=lambda node: node.priority)
        self.executing_nodes = {}
        self.remaining_parents = {n: len(n.parents) for n in self.waiting_nodes}

        self.handle_dir = Path.cwd() / "handles"
        self.handle_dir.mkdir(exist_ok=True)

    def execute(self):
        num_done = 0
        cycle_counter = itertools.count()

        while (
            len(self.waiting_nodes)
            + len(self.executable_nodes)
            + len(self.executing_nodes)
            > 0
        ):
            cycle_start = time.time()
            cycle = next(cycle_counter)

            logger.debug(f"beginning execute cycle {cycle}")

            for node in self.waiting_nodes.copy():
                if self.remaining_parents[node] == 0:
                    logger.debug(f"node {node.name} can execute")
                    self.executable_nodes.push(node)
                    self.waiting_nodes.remove(node)

            num_executed = 0
            while len(self.executable_nodes) > 0:
                if (
                    self.max_execute_per_cycle is not None
                    and num_executed >= self.max_execute_per_cycle
                ):
                    logger.debug(
                        f"not executing more nodes this cycle because hit max_execute_per_cycle ({self.max_execute_per_cycle})"
                    )
                    break

                node = self.executable_nodes.pop()
                logger.debug(f"executing node {node} with priority {node.priority}")

                if node.noop:
                    logger.debug(f"node {node} was NOOP")
                    handle = None
                else:
                    self._run_script(node, node.pre)
                    handle = self._run_node(node)

                logger.debug(f"handle for node {node} is {handle}")
                self.executing_nodes[node] = handle
                num_executed += 1

            for node, handle in self.executing_nodes.copy().items():
                if self._is_node_complete(handle):
                    self._run_script(node, node.post)
                    for child in node.children:
                        self.remaining_parents[child] -= 1
                    self.executing_nodes.pop(node)
                    num_done += 1
                    logger.debug(f"node {node} is complete")
                else:
                    logger.debug(f"node {node} is not complete")

            loop_time = time.time() - cycle_start
            sleep = max(self.min_loop_delay - loop_time, 0)

            logger.debug(f"{num_done}/{len(self.dag.nodes)} nodes are complete")
            logger.debug(
                f"finished execute cycle {cycle} in {loop_time:.6f} seconds, sleeping {sleep:.6f} seconds before next loop"
            )
            time.sleep(sleep)

        return num_done

    def _run_node(self, node: dag.Node):
        handle_path = self.handle_dir / node.name
        try:
            handle = jobs.ClusterHandle.load(handle_path)
            logger.debug(f"recovered handle {handle} for node {node}")
            return handle
        except (FileNotFoundError,):
            pass

        sub = htcondor.Submit(dict(node.submit_description))
        for k, v in node.vars.items():
            sub[k] = v

        self._inject_submit_descriptors(node, sub)

        # logger.debug(f"submit description for node {node} is\n{sub}")

        currdir = os.getcwd()
        if node.dir is not None:
            os.chdir(node.dir)

        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            result = sub.queue_with_itemdata(txn)

        os.chdir(currdir)

        handle = jobs.ClusterHandle(result)

        handle.save(handle_path)

        return handle

    def _inject_submit_descriptors(self, node, submit):
        submit["dag_node_name"] = node.name
        submit["+DAGManJobId"] = self.executor_cluster_id
        submit["submit_event_notes"] = f"DAG Node: {node.name}"
        submit["dagman_log"] = self.event_log_path.as_posix()
        # sub["+DAGManNodesMask"] = '"' # todo: getEventMask() produces this
        submit["priority"] = str(node.priority)
        # some conditional coming in to suppress node job logs
        submit["+DAGParentNodeNames"] = f"\"{' '.join(n.name for n in node.parents)}\""
        # something about DAG_STATUS
        # something about FAILED_COUNT
        # something about holding claims
        # something about suppressing notifications
        # something about accounting group and user

    def _run_script(self, node, script):
        if script is None:
            return

        processed_args = []
        for arg in script.arguments:
            arg = arg.replace("$JOB", node.name)

            processed_args.append(arg)

        logger.debug(
            f'running subprocess: "{script.executable} {" ".join(processed_args)}"'
        )
        p = subprocess.run([script.executable, *processed_args], capture_output=True)
        logger.debug(f"subprocess result for node {node} script: {p}")

    def _is_node_complete(self, handle):
        if handle is None:  # no handle means noop node
            return True
        return handle.state.is_complete()


class MockExecutor(Executor):
    def _inject_submit_descriptors(self, node, submit):
        super()._inject_submit_descriptors(node, submit)

        submit["hold"] = "true"
        submit["skip_filechecks"] = "true"

    def _is_node_complete(self, handle):
        if handle is not None:
            handle.remove()
        return True
