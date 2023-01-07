import matplotlib.pyplot as plt
import argparse
import json
from evaluation.benchmark import TYPES, PIPELINES, PERSIST_MODES, SCALE_FACTORS


def load_log(filepath):
    with open(filepath, "r") as file:
        log = json.load(file)
    return log


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Visualize")
    parser.add_argument("log", type=str, help="path to log that is to be visualized")

    args = parser.parse_args()

    log = load_log(args.log)

    for type in TYPES:
        log_type = log[type]
        for pipeline in PIPELINES[type]:
            log_pipeline = log_type[pipeline]
            for persist_mode in PERSIST_MODES[type]:
                log_persist_mode = log_pipeline[persist_mode]
                for sf in SCALE_FACTORS:
                    print(f"{type} - {pipeline} - {persist_mode} - {sf}")
