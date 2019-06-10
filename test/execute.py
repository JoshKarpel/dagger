#!/usr/bin/env python3

import logging
from pathlib import Path
import sys
import gc
import time

import dagger

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter("%(asctime)s ~ %(name)s ~ %(message)s"))
jobs_logger = logging.getLogger("htcondor_jobs")
jobs_logger.addHandler(handler)
jobs_logger.setLevel(logging.DEBUG)
dag_logger = logging.getLogger("dagger")
dag_logger.addHandler(handler)
dag_logger.setLevel(logging.DEBUG)

dag = dagger.DAG.from_file(Path("good.ldag"))

# for k, v in dag.nodes.items():
#     print(v.description())

executor = dagger.MockExecutor(dag, max_execute_per_cycle=None)
num = executor.execute()

print(f"EXECUTED {num} NODES")
time.sleep(10)
gc.collect()
time.sleep(10)
