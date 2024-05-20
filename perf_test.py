import cProfile
import pstats
import time

import pyperf

from tests.qreader_test import test_reading_compressed


def read_data():
    time.sleep(0.001)


def main():
    # runner = pyperf.Runner()
    # runner.bench_func('sleep', func)
    profiler = cProfile.Profile()
    if False:
        profiler.enable()
        test_reading_compressed()
        profiler.disable()
        stats = pstats.Stats(profiler)
        stats.dump_stats("test.prof")
    else:
        test_reading_compressed()


if __name__ == "__main__":
    main()
