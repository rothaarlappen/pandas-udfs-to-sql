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
PIPELINES = {
    "to_sql": ["very_simple_pipeline.py", "simple_pipeline.py", "medium_pipeline.py"],
    "head": [
        "head_very_simple_pipeline.py",
        "head_simple_pipeline.py",
        "head_medium_pipeline.py",
    ],
}

RELATED_WORK_PIPELINES = {
    "grizzly": 
    {   "very_simple_pipeline.py" : "grizzly_very_simple_pipeline.py",
        "simple_pipeline.py" : "grizzly_simple_pipeline.py",
        "medium_pipeline.py" : "grizzly_medium_pipeline.py"
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
                    "converted", converted_pipeline_file, benchmark_results_pipeline_persist
                )
                time_pipeline_execution(
                    "original", pipeline_file, benchmark_results_pipeline_persist
                )
                
                for system in RELATED_WORK_PIPELINES.keys():
                    third_party_pipeline = RELATED_WORK_PIPELINES[system].get(pipeline, None)
                    if third_party_pipeline is None:
                        break
                    pipeline_file = path.join(pipeline_directory, third_party_pipeline)
                    time_pipeline_execution(
                        system, pipeline_file, benchmark_results_pipeline_persist
                    )

    with open("benchmark_log.json", "a") as log:
        log.write(json.dumps(benchmark_results))


if __name__ == "__main__":
    main()
