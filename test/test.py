import htcondor_jobs as jobs

desc = jobs.SubmitDescription(
    universe="scheduler",
    executable="execute.py",
    getenv=True,
    log="test.log",
    output="test.output",
    error="test.error",
    stream_output="true",
    stream_error="true",
    **{
        "+DAGManJobID": "$(cluster)",
        "+OtherJobRemoveRequirements": "DAGManJobID =?= $(cluster)",  # todo: ?
    },
)

handle = jobs.submit(desc)
print(handle)
