"""
Yixuan Mei, 2022.07.22
This file contains structs and functions used by the scheduler.
"""
import pickle
from enum import Enum


class CommandType(Enum):
    GPU_USAGE = 0


class Packet:
    def __init__(self, command_type: CommandType):
        self.command_type = command_type


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
