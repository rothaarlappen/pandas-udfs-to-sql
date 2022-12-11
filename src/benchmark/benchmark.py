import pickle
from typing import List
from dataclasses import dataclass, asdict
import os
import timeit


@dataclass
class Benchmark:
    repetitions: int
    dataset: str
    size_columns: int
    benchmark_name: str
    benchmark_code: str
    setup_code: str = ""
    benchmark_dir: str = "./benchmarks"
    local: bool = True
    setup_times: List[float] = []
    run_times: List[float] = []

    def setup(self):
        exec(self.setup_code)

    def run(self):
        exec(self.benchmark_code)

    def run_benchmark(self):
        self.setup()
        self.run()

    def to_pickle(self):
        if not os.path.exists(self.benchmark_dir):
            os.makedirs(self.benchmark_dir)
        with open(f"{self.benchmark_dir}/{self.benchmark_name}.pickle", "wb") as file:
            pickle.dump(self, file)


if __name__ == "__main__":
    test = Benchmark(
        100,
        "test",
        100,
        "test_bench",
        """
convert('./src/testqueries/simple_pipeline.py', './benchmarks/test.py')
""",
        setup_code="""
from src.converter.convert import convert
global convert""",
    )
    test.run_benchmark()
    test.to_pickle()
    with open("./benchmarks/test_bench.pickle", "rb") as file:
        load_test = pickle.load(file)
    print(asdict(load_test))
