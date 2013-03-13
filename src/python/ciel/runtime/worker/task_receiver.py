# Copyright (c) 2010--11 Chris Smowton <Chris.Smowton@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import threading
import os
import ciel
import logging
import socket
import cPickle
from ciel.public.references import json_decode_object_hook,\
    SWReferenceJSONEncoder, reference_from_pb
import simplejson
import struct
import select
from ciel.runtime import task_pb2

def task_descriptor_from_pb(pb):
    descriptor = {'task_id': pb.task_id,
                  'handler': pb.handler,
                  'event_index': pb.event_index,
                  'job' : pb.job}
    if pb.HasField('parent'):
        descriptor['parent'] = pb.parent
    if pb.HasField('scheduling_class'):
        descriptor['scheduling_class'] = pb.scheduling_class
    if pb.HasField('type'):
        descriptor['type'] = pb.type
    if pb.HasField('task_private'):
        descriptor['task_private'] = reference_from_pb(pb.task_private)
    descriptor['expected_outputs'] = []
    descriptor['expected_outputs'].extend(pb.expected_outputs)
    descriptor['inputs'] = []
    for i in pb.inputs:
        descriptor['inputs'].append(reference_from_pb(i))
    descriptor['dependencies'] = []
    for d in pb.dependencies:
        descriptor['dependencies'].append(reference_from_pb(d))
    return descriptor

class TaskReceiverThread:

    def __init__(self, bus, worker):
        self.bus = bus
        self.thread = threading.Thread(target=self.main_loop)
        self.worker = worker
        self.data = ''
        self.control_pipe = os.pipe()

    def start(self):
        self.thread.start()

    def stop(self):
        os.write(self.control_pipe[1], 'a')

    def subscribe(self):
        self.bus.subscribe('start', self.start, 75)
        self.bus.subscribe('stop', self.stop, 10)

    def recv_n(self, n):
        while len(self.data) < n:
            events = select.select([self.control_pipe[0], self.conn], [], [])
            #print rlist, self.conn, self.conn in rlist
            if self.conn in events[0]:
                bit = self.conn.recv(4096)
                #print "recvd", bit, len(bit)
                self.data += bit
            if self.control_pipe[0] in events[0]:
                return None
        data = self.data[:n]
        self.data = self.data[n:]
        return data

    def main_loop(self):
         self.listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
         self.listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
         self.listen.bind(('', 8139))
         self.listen.listen(1)
         print "about to accept"
         self.conn, self.addr = self.listen.accept()
         self.worker.conn = self.conn
         while True:
             #print "about to load task"
             num = self.recv_n(4)
             if num is None:
                 break
             num = struct.unpack("!I", num)[0]
             data = self.recv_n(num)
             if data is None:
                 break
             #task_descriptor = simplejson.loads(data, object_hook=json_decode_object_hook)
             #task_descriptor = cPickle.loads(data)
             #print "loaded task"
             task_descriptor = task_pb2.Task()
             task_descriptor.ParseFromString(data)
             task_descriptor = task_descriptor_from_pb(task_descriptor)
             self.worker.multiworker.create_and_queue_taskset(task_descriptor)
             #print "started task"

def create_task_recv_thread(bus, worker):
    receiver = TaskReceiverThread(bus, worker)
    receiver.subscribe()
