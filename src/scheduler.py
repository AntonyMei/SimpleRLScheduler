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
    # initialize zmq socket
    zmq_context = zmq.Context()
    master_listen_socket = zmq_context.socket(zmq.PULL)
    master_address = f"tcp://*:{config.master_port_inside}"
    master_listen_socket.bind(master_address)
    print(f"[{os.getpid()}] Master listens at {config.master_port_inside}.")

    # main loop
    while True:
        raw_msg = master_listen_socket.recv()
        msg = deserialize(raw_msg)
        print(msg)


def worker():
    # initialize zmq socket
    zmq_context = zmq.Context()
    master_listen_socket = zmq_context.socket(zmq.PUSH)
    master_address = f"tcp://{config.master_ip}:{config.master_port_outside}"
    master_listen_socket.connect(master_address)
    print(f"[{os.getpid()}] Contact master at {master_address}.")

    # main loop
    counter = 0
    while True:
        msg = f"Client Message {counter}"
        master_listen_socket.send(serialize(msg))
        print(f"Client sends: {msg}")
        counter += 1
        time.sleep(1)
