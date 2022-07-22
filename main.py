"""
Yixuan Mei, 2022.07.22
This file is entry point to scheduler.
"""
from src.scheduler import master, worker
import argparse


def main():
    # parse argument
    parser = argparse.ArgumentParser(description="SimpleRLScheduler")
    parser.add_argument("--type", required=True, help="master / worker")
    args = parser.parse_args()

    # start scheduler
    if args.type == "master":
        master()
    elif args.type == "worker":
        worker()
    else:
        raise NotImplementedError


if __name__ == '__main__':
    main()
