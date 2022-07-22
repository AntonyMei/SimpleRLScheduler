"""
Yixuan Mei, 2022.07.22
This file contains the scheduler per se.
"""
import os

import zmq

import src.config as config
from src.utils import PacketType, Packet
from src.utils import get_gpu_memory
from src.utils import serialize, deserialize


def master():
    # initialize master listen socket
    zmq_context = zmq.Context()
    master_listen_socket = zmq_context.socket(zmq.PULL)
    master_address = f"tcp://*:{config.master_port_inside}"
    master_listen_socket.bind(master_address)
    print(f"[master (pid={os.getpid()})] Master listens at {config.master_port_inside}.")

    # initialize master send socket
    master_send_socket_list = []
    total_worker_count = len(config.worker_ip_list)
    cur_worker_id = 0
    for worker_ip in config.worker_ip_list:
        # initialize socket
        master_send_socket = zmq_context.socket(zmq.PUSH)
        worker_address = f"tcp://{worker_ip}:{config.worker_port_outside}"
        master_send_socket.connect(worker_address)
        master_send_socket_list.append(master_send_socket)
        print(f"[master (pid={os.getpid()})] Master connects to worker at {worker_address}.")
        # send worker id and ip to worker
        initial_packet = Packet(packet_type=PacketType.WORKER_IDENTITY,
                                additional_info=[cur_worker_id, worker_ip])
        cur_worker_id += 1
        serialized_initial_packet = serialize(initial_packet)
        master_send_socket.send(serialized_initial_packet)
    assert cur_worker_id == total_worker_count

    # start main loop
    while True:
        command = input("Scheduler >>")
        if command == "gpu":
            # prepare packet and send
            packet = Packet(packet_type=PacketType.GPU_USAGE_MASTER)
            serialized_packet = serialize(packet)
            for master_send_socket in master_send_socket_list:
                master_send_socket.send(serialized_packet)
            # gather reply from all workers
            for _ in range(total_worker_count):
                raw_reply_packet = master_listen_socket.recv()
                reply_packet = deserialize(raw_reply_packet)
                assert reply_packet.packet_type == PacketType.GPU_USAGE_WORKER
                reply_worker_id = reply_packet.additional_info[0]
                reply_worker_ip = reply_packet.additional_info[1]
                gpu_memory_list = reply_packet.additional_info[2]
                print(f"Worker {reply_worker_id} at {reply_worker_ip}: {gpu_memory_list}")
        else:
            print(f"Unknown command: {command}")


def worker():
    # initialize master listen socket
    zmq_context = zmq.Context()
    master_listen_socket = zmq_context.socket(zmq.PUSH)
    master_address = f"tcp://{config.master_ip}:{config.master_port_outside}"
    master_listen_socket.connect(master_address)
    print(f"[worker (pid={os.getpid()})] Connect to master at {master_address}.")

    # initialize worker listen socket
    worker_listen_socket = zmq_context.socket(zmq.PULL)
    worker_address = f"tcp://*:{config.worker_port_inside}"
    worker_listen_socket.bind(worker_address)
    print(f"[worker (pid={os.getpid()})] Listens at {worker_address}.")

    # determine id and ip from initial packet
    raw_initial_packet = worker_listen_socket.recv()
    initial_packet = deserialize(raw_initial_packet)
    assert initial_packet.packet_type == PacketType.WORKER_IDENTITY
    cur_worker_id = initial_packet.additional_info[0]
    cur_worker_ip = initial_packet.additional_info[1]
    print(f"Current worker is worker {cur_worker_id} at {cur_worker_ip}")

    # start main loop
    while True:
        raw_packet = worker_listen_socket.recv()
        packet = deserialize(raw_packet)
        if packet.packet_type == PacketType.GPU_USAGE_MASTER:
            gpu_memory_list = get_gpu_memory()
            reply_packet = Packet(packet_type=PacketType.GPU_USAGE_WORKER,
                                  additional_info=[cur_worker_id, cur_worker_ip, gpu_memory_list])
            serialized_reply_packet = serialize(reply_packet)
            master_listen_socket.send(serialized_reply_packet)
        else:
            print(f"Received unsupported packet with type {packet.packet_type}")
