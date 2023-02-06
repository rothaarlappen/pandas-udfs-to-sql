import sys
import time
import json
from os import path
from dotenv import set_key

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from converter import convert
from contextlib import redirect_stdout
from io import StringIO
import subprocess

pipeline_directory = path.join(
    path.dirname(path.dirname(path.abspath(__file__))), "testqueries"
)

# C:\Python310\python.exe       -> python
# /root/anaconda3/bin/python3   -> python3
# /root/anaconda3/bin/python    -> python
PYTHON = path.basename(sys.executable).split(".")[0]

DATAFRAME_COMMAND = ["to_sql", "head"]
RAW_PIPELINES = ["very_simple_pipeline", "simple_pipeline", "medium_pipeline"]
PIPELINES = {
    "to_sql": [
        "very_simple_pipeline.py",
        "simple_pipeline.py",
        "medium_pipeline.py",
        "compute_intensive_pipeline.py",
    ],
    "head": [
        "head_very_simple_pipeline.py",
        "head_simple_pipeline.py",
        "head_medium_pipeline.py",
    ],
}

RELATED_WORK_PIPELINES = {
    "scan_table_materialize": {
        "flavors": ["postgresql"],
        "pipelinemapping": {
            "very_simple_pipeline": "scan_table_materialize.py",
            "simple_pipeline": "scan_table_materialize.py",
            "medium_pipeline": "scan_table_materialize.py",
        },
    },
    "grizzly": {
        "flavors": ["postgresql"],
        "pipelinemapping": {
            "very_simple_pipeline": "grizzly_very_simple_pipeline.py",
            "simple_pipeline": "grizzly_simple_pipeline.py",
            "medium_pipeline": "grizzly_medium_pipeline.py",
        },
    },
    "sql_native": {
        "flavors": ["postgresql"],
        "pipelinemapping": {
            "very_simple_pipeline": "sql_very_simple_pipeline.py",
            "simple_pipeline": "sql_simple_pipeline.py",
            "medium_pipeline": "sql_medium_pipeline.py",
        },
    },
    "sql_server_translated": {
        "flavors": ["sqlserver"],
        "pipelinemapping": {
            "very_simple_pipeline": "sqlserver_very_simple_pipeline.py",
            "simple_pipeline": "sqlserver_simple_pipeline.py",
            "medium_pipeline": "sqlserver_medium_pipeline.py",
        },
    },
}

UDA_PIPELINES = {
    "uda_simple_pipeline.py": [
        "manually_converted_setup_uda_simple_pipeline.py",
        "manually_converted_uda_simple_pipeline.py",
    ],
}

PERSIST_MODES = {"to_sql": ["MATERIALIZED_VIEW", "NEW_TABLE"], "head": ["NONE"]}
SCALE_FACTORS = [0.01, 0.1, 1.0, 5.0, 10.0]
REPETITIONS = 5


def print_and_log(
    type: str,
    times: list,
    converted_pipeline: str,
    scale_factor: float,
    repetitions: int,
    log: dict,
):
    print(f"{path.basename(converted_pipeline)} @ {scale_factor} :  {times}")
    print(
        f"{path.basename(converted_pipeline)} @ {scale_factor} : {sum(times) / repetitions}"
    )
    print("")
    log_scalefactor = log.setdefault(scale_factor, {}).setdefault(type, {})
    log_scalefactor["times"] = times
    log_scalefactor["avg"] = sum(times) / repetitions
    log_scalefactor["med"] = sorted(times)[repetitions // 2]


def time_pipeline_execution(
    type: str,
    converted_pipeline: str,
    log: dict,
    repetitions=REPETITIONS,
    sql_flavor: str = "postgresql",
):
    f = StringIO()
    for scale_factor in SCALE_FACTORS:
        set_key(".env", "pg_scalefactor", str(scale_factor))
        set_key(".env", "flavor", str(sql_flavor))
        times = []
        for _ in range(repetitions):
            with redirect_stdout(f):
                start = time.time()
                subprocess.run(
                    f"{PYTHON} {converted_pipeline}",
                    stdout=subprocess.DEVNULL,
                    shell=True,
                )
                end = time.time()
                subprocess.run(f"{PYTHON} -m src.evaluation.teardown", shell=True)
            time_lapsed = end - start
            times.append(time_lapsed)
        print_and_log(type, times, converted_pipeline, scale_factor, repetitions, log)


def main():
    # bench this project:
    try:
        with open("data/benchmark_log.json", "r") as log:
            benchmark_results = json.load(log)
    except:
        benchmark_results = {}
    for df_command in DATAFRAME_COMMAND:
        benchmark_results_type = benchmark_results.setdefault(df_command, {})
        for pipeline in PIPELINES[df_command]:
            benchmark_results_pipeline = benchmark_results_type.setdefault(pipeline, {})
            for persist_mode in PERSIST_MODES[df_command]:
                if persist_mode in benchmark_results_pipeline:
                    print(f"cache hit {df_command} {pipeline} {persist_mode}")
                    continue
                benchmark_results_pipeline_persist = (
                    benchmark_results_pipeline.setdefault(persist_mode, {})
                )
                pipeline_file = path.join(pipeline_directory, pipeline)
                (setup_file, converted_pipeline_file) = convert.convert_pipeline(
                    pipeline_file, persist_mode
                )

                print(setup_file, converted_pipeline_file)

                time_pipeline_execution(
                    "setup", setup_file, benchmark_results_pipeline_persist
                )
                time_pipeline_execution(
                    "converted",
                    converted_pipeline_file,
                    benchmark_results_pipeline_persist,
                )
                time_pipeline_execution(
                    "original", pipeline_file, benchmark_results_pipeline_persist
                )
                # TODO: Stop executing this for every df_command/persist_mode...
                time_pipeline_execution(
                    "sqlserver",
                    pipeline_file,
                    benchmark_results_pipeline_persist,
                    sql_flavor="sqlserver",
                )
                with open("data/benchmark_log.json", "w") as log:
                    log.write(json.dumps(benchmark_results))

    # bench related work/systems:
    # ignoring different persist modes, as they don't seem to have a big impact on runtime
    try:
        with open("data/related_benchmark_log.json", "r") as log:
            related_benchmark_results = json.load(log)
    except:
        related_benchmark_results = {}
    for pipeline in RAW_PIPELINES:
        benchmark_results_pipeline = related_benchmark_results.setdefault(pipeline, {})
        for system in RELATED_WORK_PIPELINES.keys():
            benchmark_results_system = benchmark_results_pipeline.setdefault(system, {})
            for sql_flavor in RELATED_WORK_PIPELINES[system]["flavors"]:
                if sql_flavor in benchmark_results_pipeline:
                    print(f"cache hit {pipeline} {system} {sql_flavor}")
                    continue
                benchmark_results_flavor = benchmark_results_system.setdefault(
                    sql_flavor, {}
                )

                third_party_pipeline = RELATED_WORK_PIPELINES[system][
                    "pipelinemapping"
                ].get(pipeline, None)
                if third_party_pipeline is None:
                    continue
                pipeline_file = path.join(pipeline_directory, third_party_pipeline)
                time_pipeline_execution(
                    system,
                    pipeline_file,
                    benchmark_results_flavor,
                    sql_flavor=sql_flavor,
                )
                with open("data/related_benchmark_log.json", "w") as log:
                    log.write(json.dumps(related_benchmark_results))

    try:
        with open("data/uda_benchmark_results.json", "r") as log:
            uda_benchmark_results = json.load(log)
    except:
        uda_benchmark_results = {}
    for pipeline in UDA_PIPELINES.keys():
        benchmark_results_type = uda_benchmark_results.setdefault(pipeline, {})

        pipeline_file = path.join(pipeline_directory, pipeline)
        setup_file = path.join(pipeline_directory, UDA_PIPELINES[pipeline][0])
        converted_pipeline_file = path.join(
            pipeline_directory, UDA_PIPELINES[pipeline][1]
        )

        time_pipeline_execution("setup", setup_file, benchmark_results_type)
        time_pipeline_execution(
            "converted",
            converted_pipeline_file,
            benchmark_results_type,
        )
        time_pipeline_execution("original", pipeline_file, benchmark_results_type)
    with open("data/uda_benchmark_results.json", "w") as log:
        log.write(json.dumps(uda_benchmark_results))


if __name__ == "__main__":
    main()
