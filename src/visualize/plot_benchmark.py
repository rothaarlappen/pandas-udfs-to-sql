import matplotlib.pyplot as plt
import argparse
import json
import numpy as np
from evaluation.benchmark import (
    DATAFRAME_COMMAND,
    PIPELINES,
    PERSIST_MODES,
    SCALE_FACTORS,
)

PIPELINE_TO_COUNT = {
    "very_simple_pipeline.py": 1,
    "simple_pipeline.py": 3,
    "medium_pipeline.py": 10,
    "head_very_simple_pipeline.py": 1,
    "head_simple_pipeline.py": 3,
    "head_medium_pipeline.py": 10,
}
TYPES = ["converted", "original"]  # , "setup"]


def load_log(filepath):
    with open(filepath, "r") as file:
        log = json.load(file)
    return log


def get_all_combinations(log):
    for df_command in DATAFRAME_COMMAND:
        log_df_command = log[df_command]
        for pipeline in PIPELINES[df_command]:
            log_pipeline = log_df_command[pipeline]
            for persist_mode in PERSIST_MODES[df_command]:
                log_persist_mode = log_pipeline[persist_mode]
                for sf in SCALE_FACTORS:
                    log_sf = log_persist_mode[str(sf)]
                    for type in TYPES:
                        benchmark = log_sf[type]
                        yield {
                            "type": type,
                            "df_command": df_command,
                            "pipeline": pipeline,
                            "sf": sf,
                            "persist_mode": persist_mode,
                            "count": PIPELINE_TO_COUNT[pipeline],
                            "runtimes": benchmark["times"],
                            "med": benchmark["med"],
                        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Visualize")
    parser.add_argument("log", type=str, help="path to log that is to be visualized")

    args = parser.parse_args()

    log = load_log(args.log)

    for benchmark in get_all_combinations(log):
        print(
            f"{benchmark['df_command']} - {benchmark['pipeline']} - {benchmark['sf']} - {benchmark['count']} - {benchmark['runtimes']}"
        )

    runtimes_on_scalefactor_by_udf = {}
    for benchmark in get_all_combinations(log):
        runtimes_on_scalefactor_by_udf.setdefault(
            benchmark["df_command"], {}
        ).setdefault(benchmark["persist_mode"], {}).setdefault(
            benchmark["count"], {}
        ).setdefault(
            benchmark["pipeline"], {}
        ).setdefault(
            benchmark["type"], {}
        ).setdefault(
            benchmark["sf"], []
        ).append(
            benchmark["med"]
        )
    print(runtimes_on_scalefactor_by_udf)
    r = np.arange(len(SCALE_FACTORS))
    width = 0.25
    offset = (len(TYPES) - 1) * width / 2
    for df_command in DATAFRAME_COMMAND:
        for persist_mode in PERSIST_MODES[df_command]:
            for pipeline in PIPELINES[df_command]:
                bars = []
                labels = []
                for (i, type) in enumerate(TYPES):
                    bars.append(
                        plt.bar(
                            r + width * i - offset,
                            np.asarray(
                                list(
                                    runtimes_on_scalefactor_by_udf[df_command][
                                        persist_mode
                                    ][PIPELINE_TO_COUNT[pipeline]][pipeline][
                                        type
                                    ].values()
                                )
                            ).flatten(),
                            width,
                        )
                    )
                    labels.append(f"{type}")
                plt.xticks(r, SCALE_FACTORS)
                plt.xlabel("Scalefactor")
                plt.legend(bars, labels)
                plt.title(
                    f"{df_command} Runtimes @ {PIPELINE_TO_COUNT[pipeline]} UDF {persist_mode  if (persist_mode != 'NONE') else ''}"
                )
                plt.savefig(
                    f"plots/{df_command}_runtimes_{PIPELINE_TO_COUNT[pipeline]}_UDF_{persist_mode}.pdf"
                )
                plt.show()
