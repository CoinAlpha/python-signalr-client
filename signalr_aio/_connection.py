#!/usr/bin/python
# -*- coding: utf-8 -*-

# signalr_aio/_connection.py
# Stanislav Lazarov
import asyncio

from .events import EventHook
from .hubs import Hub
from .transports import Transport
import logging

class Connection(object):
    protocol_version = '1.5'
    _connection_logger = None

    @classmethod
    def logger(cls):
        if cls._connection_logger is None:
            cls._connection_logger = logging.getLogger(__name__)
        return cls._connection_logger

    def __init__(self, url, session=None):
        self.url = url
        self.__hubs = {}
        self.__send_counter = -1
        self.hub = None
        self.session = session
        self.received = EventHook()
        self.error = EventHook()
        self.__transport = Transport(self)
        self.started = False
        self.msg_queue = asyncio.Queue()

        async def handle_error(**data):
            error = data.get("E", None)
            if error is not None:
                self.logger().error(f"Signalr connection error: {error}")
                await self.error.fire(error)

        self.received += handle_error

    def start(self):
        self.msg_queue = asyncio.Queue()
        self.hub = [hub_name for hub_name in self.__hubs][0]
        self.__transport.start()

    def register_hub(self, name):
        if name not in self.__hubs:
            if self.started:
                raise RuntimeError(
                    'Cannot create new hub because connection is already started.')
            self.__hubs[name] = Hub(name, self)
            return self.__hubs[name]

    def increment_send_counter(self):
        self.__send_counter += 1
        return self.__send_counter

    def send(self, message):
        self.__transport.send(message)

    def close(self):
        self.__transport.close()
