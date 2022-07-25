import argparse


def main():
    # parse argument
    parser = argparse.ArgumentParser(description="SampleWorkload")
    parser.add_argument("--type", required=True, type=str, help="trainer / worker")
    parser.add_argument("--worker_id", required=True, type=int, help="worker id")
    args = parser.parse_args()

    # start scheduler
    if args.type == "trainer":
        print("Sample workload: trainer starts")
        return "Trainer Finishes"
    elif args.type == "worker":
        cur_worker_id = args.worker_id
        print(f"Sample workload: worker {cur_worker_id} starts")
        return f"Worker {cur_worker_id} Finishes"
    else:
        raise NotImplementedError


if __name__ == '__main__':
    main()
