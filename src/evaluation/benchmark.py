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
    "to_sql": ["very_simple_pipeline.py", "simple_pipeline.py", "medium_pipeline.py"],
    "head": [
        "head_very_simple_pipeline.py",
        "head_simple_pipeline.py",
        "head_medium_pipeline.py",
    ],
}

RELATED_WORK_PIPELINES = {
    "scan_table": {
        "very_simple_pipeline": "scan_table.py",
        "simple_pipeline": "scan_table.py",
        "medium_pipeline": "scan_table.py",
    },
    "grizzly": {
        "very_simple_pipeline": "grizzly_very_simple_pipeline.py",
        "simple_pipeline": "grizzly_simple_pipeline.py",
        "medium_pipeline": "grizzly_medium_pipeline.py",
    },
    "sql_native": {
        "very_simple_pipeline": "sql_very_simple_pipeline.py",
        "simple_pipeline": "sql_simple_pipeline.py",
        "medium_pipeline": "sql_medium_pipeline.py",
    }
    # Microsoft etc. tbd.
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
    type: str, converted_pipeline: str, log: dict, repetitions=REPETITIONS
):
    f = StringIO()
    for scale_factor in SCALE_FACTORS:
        set_key(".env", "pg_scalefactor", str(scale_factor))
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
    benchmark_results = {}
    for df_command in DATAFRAME_COMMAND:
        benchmark_results_type = benchmark_results.setdefault(df_command, {})
        for pipeline in PIPELINES[df_command]:
            benchmark_results_pipeline = benchmark_results_type.setdefault(pipeline, {})
            for persist_mode in PERSIST_MODES[df_command]:
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

    # bench related work/systems:
    # ignoring different persist modes, as they don't seem to have a big impact on runtime
    related_benchmark_results = {}
    for pipeline in RAW_PIPELINES:
        benchmark_results_pipeline = related_benchmark_results.setdefault(pipeline, {})
        for system in RELATED_WORK_PIPELINES.keys():
            benchmark_results_system = benchmark_results_pipeline.setdefault(system, {})
            third_party_pipeline = RELATED_WORK_PIPELINES[system].get(pipeline, None)
            if third_party_pipeline is None:
                print(f"Pipeline mapping missing for {pipeline} in {system}")
                break
            pipeline_file = path.join(pipeline_directory, third_party_pipeline)
            time_pipeline_execution(system, pipeline_file, benchmark_results_system)

    with open("data/benchmark_log.json", "a") as log:
        log.write(json.dumps(benchmark_results))

    with open("data/related_benchmark_log.json", "a") as log:
        log.write(json.dumps(related_benchmark_results))


if __name__ == "__main__":
    main()
