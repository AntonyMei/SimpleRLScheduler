"""
Yixuan Mei, 2022.07.22
This file contains structs and functions used by the scheduler.
"""
import pickle
import subprocess as sp
from enum import Enum, auto


class PacketType(Enum):
    WORKER_IDENTITY = 0
    GPU_USAGE_QUERY = auto()
    GPU_USAGE_REPLY = auto()
    START_QUERY = auto()
    START_REPLY = auto()


class Packet:
    def __init__(self, packet_type: PacketType, additional_info=None):
        self.packet_type = packet_type
        self.additional_info = additional_info


def serialize(obj, file=None):
    """
    This is a general serializer based on pickle. Note that (list of) numpy arrays
    should use serialize_numpy(_list) instead for better performance.

    :param obj: object to be serialized
    :param file: file to write the stream into
    :return: data_stream if file is None
             no return if file is not None
    """
    if file is None:
        data_stream = pickle.dumps(obj=obj, protocol=pickle.HIGHEST_PROTOCOL)
        return data_stream
    else:
        pickle.dump(obj=obj, file=file, protocol=pickle.HIGHEST_PROTOCOL)


def deserialize(data_stream):
    """
    This is a general deserializer based on pickle. Note that data_stream must be
    the result of previous calls to serialize.

    :param data_stream: data stream to be deserialized
    :return: deserialized_object
    """
    deserialized_object = pickle.loads(data=data_stream)
    return deserialized_object


def get_gpu_memory():
    """
    Returns available gpu memory for each available gpu
    https://stackoverflow.com/questions/59567226/how-to-programmatically-determine-available-gpu-memory-with-tensorflow
    """

    # internal tool function
    def _output_to_list(x):
        return x.decode('ascii').split('\n')[:-1]

    command = "nvidia-smi --query-gpu=memory.free --format=csv"
    memory_free_info = _output_to_list(sp.check_output(command.split()))[1:]
    memory_free_values = [int(x.split()[0]) for i, x in enumerate(memory_free_info)]
    return memory_free_values
