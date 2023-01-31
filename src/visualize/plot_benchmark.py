import matplotlib.pyplot as plt
import json
import numpy as np


from src.evaluation.benchmark import (
    DATAFRAME_COMMAND,
    PIPELINES,
    PERSIST_MODES,
    RAW_PIPELINES,
    RELATED_WORK_PIPELINES,
    SCALE_FACTORS,
)

PIPELINE_TO_COUNT = {
    "very_simple_pipeline": 1,
    "simple_pipeline": 3,
    "medium_pipeline": 10,
    "very_simple_pipeline.py": 1,
    "simple_pipeline.py": 3,
    "medium_pipeline.py": 10,
    "head_very_simple_pipeline.py": 1,
    "head_simple_pipeline.py": 3,
    "head_medium_pipeline.py": 10,
}

MAP_EXTERNAL_PIPELINE = {
    "head_very_simple_pipeline.py": "very_simple_pipeline",
    "head_simple_pipeline.py": "simple_pipeline",
    "head_medium_pipeline.py": "medium_pipeline",
    "very_simple_pipeline.py": "very_simple_pipeline",
    "simple_pipeline.py": "simple_pipeline",
    "medium_pipeline.py": "medium_pipeline",
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


def get_all_external_combinations(log):
    for pipeline in RAW_PIPELINES:
        pipeline_log = log[pipeline]
        for system in RELATED_WORK_PIPELINES.keys():
            system_log = pipeline_log[system]
            for sf in SCALE_FACTORS:
                benchmark = system_log[str(sf)][system]
                yield {
                    "pipeline": pipeline,
                    "sf": sf,
                    "system": system,
                    "count": PIPELINE_TO_COUNT[pipeline],
                    "runtimes": benchmark["times"],
                    "med": benchmark["med"],
                }


if __name__ == "__main__":
    log = load_log("data/benchmark.json")
    grizzly_log = load_log("data/external_benchmark.json")

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
            benchmark["sf"], benchmark["med"]
        )

    grizzly_runtimes_on_scalefactor_by_udf = {}
    native_runtimes_on_scalefactor_by_udf = {}
    scan_runtimes_on_scalefactor_by_udf = {}
    for benchmark in get_all_external_combinations(grizzly_log):
        if benchmark["system"] == "grizzly":
            grizzly_runtimes_on_scalefactor_by_udf.setdefault(
                benchmark["count"], {}
            ).setdefault(benchmark["pipeline"], {}).setdefault(
                benchmark["sf"], benchmark["med"]
            )
        if benchmark["system"] == "sql_native":
            native_runtimes_on_scalefactor_by_udf.setdefault(
                benchmark["count"], {}
            ).setdefault(benchmark["pipeline"], {}).setdefault(
                benchmark["sf"], benchmark["med"]
            )
        if benchmark["system"] == "scan":
            scan_runtimes_on_scalefactor_by_udf.setdefault(
                benchmark["count"], {}
            ).setdefault(benchmark["pipeline"], {}).setdefault(
                benchmark["sf"], benchmark["med"]
            )

    print(runtimes_on_scalefactor_by_udf)
    print(grizzly_runtimes_on_scalefactor_by_udf)

    r = np.arange(len(SCALE_FACTORS))
    width = 0.6
    for df_command in DATAFRAME_COMMAND:
        for pipeline in PIPELINES[df_command]:
            original_plotted = False
            bars_per_point = 0
            bars = []
            labels = []
            if df_command == "to_sql":
                bars.append(
                    {
                        "data": np.asarray(
                            list(
                                native_runtimes_on_scalefactor_by_udf[
                                    PIPELINE_TO_COUNT[pipeline]
                                ][MAP_EXTERNAL_PIPELINE[pipeline]].values()
                            )
                        ),
                        "name": "native",
                    }
                )
                bars.append(
                    {
                        "data": np.asarray(
                            list(
                                scan_runtimes_on_scalefactor_by_udf[
                                    PIPELINE_TO_COUNT[pipeline]
                                ][MAP_EXTERNAL_PIPELINE[pipeline]].values()
                            )
                        ),
                        "name": "scan_table_materialize",
                    }
                )
                labels.append("native SQL")
                labels.append("scan Table & materialize")
                bars_per_point += 2
            if df_command == "head":

                bars.append(
                    {
                        "data": np.asarray(
                            list(
                                grizzly_runtimes_on_scalefactor_by_udf[
                                    PIPELINE_TO_COUNT[pipeline]
                                ][MAP_EXTERNAL_PIPELINE[pipeline]].values()
                            )
                        ),
                        "name": "grizzly",
                    }
                )
                labels.append("grizzly")
                bars_per_point += 1
            for persist_mode in PERSIST_MODES[df_command]:
                for type in TYPES:
                    if type == "original":
                        if original_plotted:
                            continue
                        original_plotted = True
                    bars.append(
                        {
                            "data": np.asarray(
                                list(
                                    runtimes_on_scalefactor_by_udf[df_command][
                                        persist_mode
                                    ][PIPELINE_TO_COUNT[pipeline]][pipeline][
                                        type
                                    ].values()
                                )
                            ),
                            "name": type,
                        }
                    )
                    labels.append(
                        f"{type}{f' - {persist_mode}'  if (persist_mode != 'NONE') else ''}"
                    )
                    bars_per_point += 1
            if df_command == "to_sql":
                bars, labels = zip(
                    *sorted(zip(bars, labels), key=lambda x: x[0]["name"])
                )

            bar_references = []
            bar_width = width / bars_per_point
            offset = (bars_per_point - 1) * bar_width / 2

            for i, bar in enumerate(bars):
                bar_references.append(
                    plt.bar(r + bar_width * i - offset, bar["data"], bar_width)
                )
            plt.xticks(r, SCALE_FACTORS)
            plt.xlabel("Scalefactor")
            plt.ylabel("Seconds")
            if df_command == "to_sql":
                plt.yscale("log")
            plt.legend(bar_references, labels)
            plt.title(f"{df_command} Runtimes @ {PIPELINE_TO_COUNT[pipeline]} UDF")
            plt.savefig(
                f"src/plots/{df_command}_runtimes_{PIPELINE_TO_COUNT[pipeline]}_UDF.png"
            )
            plt.show()
