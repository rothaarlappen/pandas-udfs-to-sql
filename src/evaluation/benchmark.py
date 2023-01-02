import sys
import time
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

pipelines = ["very_simple_pipeline.py", "simple_pipeline.py", "medium_pipeline.py"]


def print_and_log(
    times: list, converted_pipeline: str, scale_factor: float, repetitions: int
):
    print(f"{path.basename(converted_pipeline)} @ {scale_factor} :  {times}")
    print(
        f"{path.basename(converted_pipeline)} @ {scale_factor} : {sum(times) / repetitions}"
    )
    print("")
    with open("benchmark.log", "a") as log:
        log.write(f"{path.basename(converted_pipeline)} @ {scale_factor} :  {times}\n")
        log.write(
            f"{path.basename(converted_pipeline)} @ {scale_factor} : {sum(times) / repetitions}\n"
        )
        log.write("\n")


def time_pipeline_execution(converted_pipeline: str, repetitions=20):
    f = StringIO()
    for scale_factor in [0.01, 0.1, 1.0]:
        set_key(".env", "pg_scalefactor", str(scale_factor))
        times = []
        for i in range(repetitions):
            with redirect_stdout(f):
                start = time.time()
                subprocess.run(
                    "python3 " + converted_pipeline,
                    stdout=subprocess.DEVNULL,
                    shell=True,
                )
                end = time.time()
            time_lapsed = end - start
            times.append(time_lapsed)
        print_and_log(times, converted_pipeline, scale_factor, repetitions)


def main():
    for pipeline in pipelines:
        with open("benchmark.log", "a") as log:
            log.write(f"pipeline: {pipeline}\n")
        pipeline = path.join(pipeline_directory, pipeline)
        (setup_file, pipeline_file) = convert.convert_pipeline(pipeline)

        print(setup_file, pipeline_file)

        time_pipeline_execution(setup_file)
        time_pipeline_execution(pipeline_file)
        time_pipeline_execution(pipeline)


if __name__ == "__main__":
    main()
