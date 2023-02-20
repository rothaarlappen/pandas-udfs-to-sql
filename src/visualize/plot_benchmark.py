import matplotlib.pyplot as plt
import json
import numpy as np
import scienceplots

plt.style.use(["science"])


from src.evaluation.benchmark import (
    DATAFRAME_COMMAND,
    PIPELINES,
    PERSIST_MODES,
    RAW_PIPELINES,
    RELATED_WORK_PIPELINES,
    SCALE_FACTORS,
)

RE_LABELS = {"original": "pandas", "converted": "pardus"}


def rename_labels(labels):
    rename_labels = []
    for label in labels:
        rename_labels.append(RE_LABELS.setdefault(label, label))
    return rename_labels


ROW_COUNTS = np.around(np.array(SCALE_FACTORS) * 1.5, decimals=3)
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
    "compute_intensive_pipeline.py": 1,
}

MAP_EXTERNAL_PIPELINE = {
    "head_very_simple_pipeline.py": "very_simple_pipeline",
    "head_simple_pipeline.py": "simple_pipeline",
    "head_medium_pipeline.py": "medium_pipeline",
    "very_simple_pipeline.py": "very_simple_pipeline",
    "simple_pipeline.py": "simple_pipeline",
    "medium_pipeline.py": "medium_pipeline",
    "compute_intensive_pipeline.py": "hard_pipeline",
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
                    for type in TYPES + ["setup"]:
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
            for flavor in system_log.keys():
                flavor_log = system_log[flavor]
                for sf in SCALE_FACTORS:
                    benchmark = flavor_log[str(sf)][system]
                    yield {
                        "pipeline": pipeline,
                        "sf": sf,
                        "system": system,
                        "count": PIPELINE_TO_COUNT[pipeline],
                        "runtimes": benchmark["times"],
                        "med": benchmark["med"],
                    }


def plot_uda_scales():
    uda_log = load_log("data/uda_benchmark_results.json")
    r = np.arange(len(SCALE_FACTORS))
    width = 0.4

    bars_per_point = 2
    bars = []
    labels = TYPES + ["setup"]

    for type in TYPES + ["setup"]:
        runtimes = []
        for sf in SCALE_FACTORS:
            runtimes.append(uda_log["uda_simple_pipeline.py"][str(sf)][type]["med"])
        bars.append(runtimes)

    # add setup times to converted:
    for i in range(len(SCALE_FACTORS)):
        bars[0][i] += bars[2][i]
    bar_references = []
    bar_width = width / bars_per_point
    offset = (bars_per_point - 1) * bar_width / 2

    for i, bar in zip([0, 1, 0], bars):

        bar_references.append(
            plt.bar(r + bar_width * i - offset, bar, bar_width, zorder=3)
        )
    plt.grid(zorder=0)
    plt.xticks(r, ROW_COUNTS)
    plt.xlabel("\#Rows (millions)")
    plt.ylabel("End-to-end Runtime (s)")
    plt.yscale("log")
    plt.legend(
        bar_references,
        rename_labels(labels),
        loc="center left",
        bbox_to_anchor=(1, 0.5),
        fontsize=12,
    )
    # plt.title(f"UDA converted vs original e2e runtimes")
    plt.savefig(f"src/plots/uda_runtimes_end_to_end.pdf")
    plt.show()


def plot_compute_intense(log):

    r = np.arange(len(SCALE_FACTORS))
    width = 0.4

    bars_per_point = 2
    bars = []
    labels = TYPES + ["setup"]

    for type in TYPES + ["setup"]:
        runtimes = []
        for sf in SCALE_FACTORS:
            runtimes.append(
                log["to_sql"]["compute_intensive_pipeline.py"]["MATERIALIZED_VIEW"][
                    str(sf)
                ][type]["med"]
            )
        bars.append(runtimes)

    # add setup times to converted:
    for i in range(len(SCALE_FACTORS)):
        bars[0][i] += bars[2][i]
    bar_references = []
    bar_width = width / bars_per_point
    offset = (bars_per_point - 1) * bar_width / 2

    for i, bar in zip([0, 1, 0], bars):

        bar_references.append(
            plt.bar(r + bar_width * i - offset, bar, bar_width, zorder=2)
        )
    plt.xticks(r, ROW_COUNTS)
    plt.grid(zorder=0)
    plt.xlabel("\#Rows (millions)")
    plt.ylabel("End-to-end Runtime (s)")
    plt.yscale("log")
    plt.legend(
        bar_references,
        rename_labels(labels),
        loc="center left",
        bbox_to_anchor=(1, 0.5),
        fontsize=12,
    )
    # plt.title(f"Compute intensive e2e runtimes")
    plt.savefig(f"src/plots/compute_intensive_end_to_end.pdf")
    plt.show()


def plot_ms_sql(log, log_original):

    r = np.arange(len(SCALE_FACTORS))
    width = 0.7

    bars_per_point = 5
    bars = []
    labels = [
        "1 UDF converted",
        "1 UDF pandas",
        "3 UDF converted",
        "3 UDF pandas",
        "10 UDF converted",
        "10 UDF pandas",
    ]

    original_mapping = {
        "very_simple_pipeline": "head_very_simple_pipeline.py",
        "simple_pipeline": "head_simple_pipeline.py",
        "medium_pipeline": "head_medium_pipeline.py",
    }
    for pipeline in RAW_PIPELINES:
        runtimes = []
        runtimes_original = []
        for sf in SCALE_FACTORS:
            runtimes.append(log[PIPELINE_TO_COUNT[pipeline]][pipeline][sf])
            runtimes_original.append(
                log_original["head"]["NONE"][PIPELINE_TO_COUNT[pipeline]][
                    original_mapping[pipeline]
                ]["original"][sf]
            )
        bars.append(runtimes)
        bars.append(runtimes_original)

    bar_references = []
    bar_width = width / bars_per_point
    offset = (bars_per_point - 1) * bar_width / 2

    for i, bar in enumerate(bars):
        bar_references.append(
            plt.bar(r + bar_width * i - offset, bar, bar_width, zorder=2)
        )
    plt.grid(zorder=0)
    plt.xticks(r, ROW_COUNTS)
    plt.xlabel("\#Rows (millions)")
    plt.ylabel("End-to-end Runtime (s)")
    plt.yscale("log")
    plt.legend(
        bar_references,
        rename_labels(labels),
        loc="center left",
        bbox_to_anchor=(1, 0.5),
    )
    # plt.title(f"Microsoft SQL runtimes at different UDF counts")
    plt.savefig(f"src/plots/Microsoft_SQL_SERVER.pdf")
    plt.show()


def plot_loc():
    r = np.arange(1)
    width = 0.1

    bars_per_point = 3
    bars = []
    labels = ["benchmark", "converter", "visualize"]

    bars = [[890], [319], [388]]
    bar_references = []
    bar_width = width / bars_per_point
    offset = (bars_per_point - 1) * bar_width / 2

    for i, bar in enumerate(bars):
        bar_references.append(plt.bar(r + bar_width * i - offset, bar, bar_width))
    plt.xticks(r, [1])
    plt.xlabel("")
    plt.ylabel("LOC")
    plt.legend(bar_references, rename_labels(labels))
    # plt.title(f"LOC for different parts of the system")
    plt.savefig(f"src/plots/loc.pdf")
    plt.show()


if __name__ == "__main__":
    log = load_log("data/benchmark_log.json")
    related_log = load_log("data/related_benchmark_log.json")

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
    ms_sql_runtimes_on_scalefactor = {}
    for benchmark in get_all_external_combinations(related_log):
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
        if benchmark["system"] == "scan_table_materialize":
            scan_runtimes_on_scalefactor_by_udf.setdefault(
                benchmark["count"], {}
            ).setdefault(benchmark["pipeline"], {}).setdefault(
                benchmark["sf"], benchmark["med"]
            )
        if benchmark["system"] == "sql_server_translated":
            ms_sql_runtimes_on_scalefactor.setdefault(
                benchmark["count"], {}
            ).setdefault(benchmark["pipeline"], {}).setdefault(
                benchmark["sf"], benchmark["med"]
            )

    r = np.arange(len(SCALE_FACTORS))
    width = 0.6
    for df_command in DATAFRAME_COMMAND:
        fig, axes = plt.subplots(nrows=1, ncols=3, sharey=True, figsize=(20, 5))
        axes[0].tick_params(labelsize=15)
        for i, pipeline in enumerate(PIPELINES[df_command]):
            original_plotted = False
            bars_per_point = 0
            bars = []
            labels = []
            if df_command == "to_sql":
                if pipeline == "compute_intensive_pipeline.py":
                    continue
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
                labels.append("copy data")
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
            if df_command == "to_sql":
                persist_mode = "NEW_TABLE"
            else:
                persist_mode = "NONE"
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
                                ][PIPELINE_TO_COUNT[pipeline]][pipeline][type].values()
                            )
                        ),
                        "name": type,
                    }
                )
                labels.append(f"{type}")
                bars_per_point += 1
            if df_command == "to_sql":
                bars, labels = zip(
                    *sorted(zip(bars, labels), key=lambda x: x[0]["data"][-1])
                )

            bar_references = []
            bar_width = width / bars_per_point
            offset = (bars_per_point - 1) * bar_width / 2

            for j, bar in enumerate(bars):
                bar_references.append(
                    axes[i].bar(
                        r + bar_width * j - offset, bar["data"], bar_width, zorder=2
                    )
                )
            axes[i].set_xticks(r, ROW_COUNTS, fontsize=15)
            axes[i].set_yscale("log")
            axes[i].grid(zorder=0)
            labels = rename_labels(labels)
            if i == 2:
                plt.legend(
                    bar_references,
                    rename_labels(labels),
                    loc="center left",
                    bbox_to_anchor=(1, 0.5),
                    fontsize=18,
                )
            # plt.title(f"{df_command} Runtimes @ {PIPELINE_TO_COUNT[pipeline]} UDF")
        for i, title in enumerate(["(a)", "(b)", "(c)"]):
            axes[i].set_title(title, fontsize=20)
        axes[1].set_xlabel("\#Rows (millions)", fontsize=20)
        axes[0].set_ylabel("End-to-end Runtime (s)", fontsize=20)
        plt.savefig(f"src/plots/{df_command}_runtimes_UDF.pdf")
        plt.show()
    plot_uda_scales()
    plot_compute_intense(log)
    plot_loc()
    plot_ms_sql(ms_sql_runtimes_on_scalefactor, runtimes_on_scalefactor_by_udf)
