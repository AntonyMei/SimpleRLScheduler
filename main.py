"""
Yixuan Mei, 2022.07.22
This file is entry point to scheduler.
"""
import argparse

from src.scheduler import master, worker, trainer


def main():
    # parse argument
    parser = argparse.ArgumentParser(description="SimpleRLScheduler")
    parser.add_argument("--type", required=True, help="master / trainer / worker")
    args = parser.parse_args()

    # start scheduler
    if args.type == "master":
        master()
    elif args.type == "worker":
        worker()
    elif args.type == "trainer":
        trainer()
    else:
        raise NotImplementedError


if __name__ == '__main__':
    main()
