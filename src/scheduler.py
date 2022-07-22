"""
Yixuan Mei, 2022.07.22
This file contains the scheduler per se.
"""
import os
import time

import zmq
import src.config as config
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
    for worker_ip in config.worker_ip_list:
        master_send_socket = zmq_context.socket(zmq.PUSH)
        worker_address = f"tcp://{worker_ip}:{config.worker_port_outside}"
        master_send_socket.connect(worker_address)
        master_send_socket_list.append(master_send_socket)
        print(f"[master (pid={os.getpid()})] Master connects to worker at {worker_address}.")

    # start main loop
    while True:
        time.sleep(1)


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

    # start main loop
    while True:
        time.sleep(1)
