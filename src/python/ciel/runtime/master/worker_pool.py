# Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
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
from __future__ import with_statement
from Queue import Queue
from ciel.runtime.pycurl_rpc import post_string_noreturn, get_string
import ciel
import datetime
import logging
import random
import simplejson
import threading
import uuid
from urlparse import urlparse
import socket
import cPickle
import struct
from ciel.public.references import json_decode_object_hook,\
    SWReferenceJSONEncoder, reference_from_pb
from ciel.runtime import task_pb2

class FeatureQueues:
    def __init__(self):
        self.queues = {}
        self.streaming_queues = {}
        
    def get_queue_for_feature(self, feature):
        try:
            return self.queues[feature]
        except KeyError:
            queue = Queue()
            self.queues[feature] = queue
            return queue

    def get_streaming_queue_for_feature(self, feature):
        try:
            return self.streaming_queues[feature]
        except KeyError:
            queue = Queue()
            self.streaming_queues[feature] = queue
            return queue

class Worker:
    
    def __init__(self, worker_id, worker_descriptor, feature_queues, worker_pool):
        self.id = worker_id
        self.netloc = worker_descriptor['netloc']
        self.features = worker_descriptor['features']
        self.scheduling_classes = worker_descriptor['scheduling_classes']
        self.last_ping = datetime.datetime.now()
        self.failed = False
        self.worker_pool = worker_pool

    def idle(self):
        pass

    def get_effective_scheduling_class(self, scheduling_class):
        if scheduling_class in self.scheduling_classes:
            return scheduling_class
        else:
            return '*'

    def get_effective_scheduling_class_capacity(self, scheduling_class):
        try:
            return self.scheduling_classes[scheduling_class]
        except KeyError:
            return self.scheduling_classes['*']

    def __repr__(self):
        return 'Worker(%s)' % self.id

    def as_descriptor(self):
        return {'worker_id': self.id,
                'netloc': self.netloc,
                'features': self.features,
                'last_ping': self.last_ping.ctime(),
                'failed':  self.failed}

def taskdict_from_pb(pb):
    descriptor = {'task_id': pb.task_id,
                  'handler': pb.handler}
    if pb.HasField('task_private'):
        descriptor['task_private'] = reference_from_pb(pb.task_private)
    if pb.HasField('worker_private'):
        descriptor['worker_private'] = {'hint' : pb.worker_private.hint}
    descriptor['expected_outputs'] = []
    descriptor['expected_outputs'].extend(pb.expected_outputs)
    descriptor['inputs'] = []
    descriptor['dependencies'] = []
    for d in pb.dependencies:
        descriptor['dependencies'].append(reference_from_pb(d))
    return descriptor

def report_from_pb(pb):
    report_data = []
    for tr_pb in pb.tr:
        spawned = []
        for s in tr_pb.spawned_tasks:
            spawned.append(taskdict_from_pb(s))
        published = []
        for p in tr_pb.published_refs:
            published.append(reference_from_pb(p))
        profiling = {'STARTED' : tr_pb.profiling.started,
                     'FINISHED' : tr_pb.profiling.finished,
                     'CREATED' : tr_pb.profiling.created,
                     'FETCHED' : {}}
        for location in tr_pb.profiling.fetched:
            profiling['FETCHED'][location.hostname] = location.duration
        report_data.append((tr_pb.task_id, tr_pb.success, (spawned, published, profiling)))
    return report_data

class RecvThread:
    def __init__(self, worker, job_pool):
        self.thread = threading.Thread(target=self.main_loop)
        self.job_pool = job_pool
        self.worker = worker
        self.data = ''

    def start(self):
        self.thread.start()

    def recv_n(self, n):
        while len(self.data) < n:
            bit = self.worker.conn.recv(4096)
            #print "recvd", bit, len(bit)
            self.data += bit
        data = self.data[:n]
        self.data = self.data[n:]
        return data

    def main_loop(self):
         while True:
             num = self.recv_n(4)
             num = struct.unpack("!I", num)[0]
             data = self.recv_n(num)
             report = task_pb2.Report()
             report.ParseFromString(data)
             try:
                 job = self.job_pool.get_job_by_id(report.job_id)
             except KeyError:
                 ciel.log('No such job: %s' % report.job_id, 'MASTER', logging.ERROR)
                 raise HTTPError(404)
             try:
                 task = job.task_graph.get_task(report.task_id)
             except KeyError:
                 ciel.log('No such task: %s in job: %s' % (report.task_id, report.job_id), 'MASTER', logging.ERROR)
                 raise HTTPError(404)
             report_payload = report_from_pb(report)
             job.report_tasks(report_payload, task, self.worker)

        
class WorkerPool:
    
    def __init__(self, bus, deferred_worker, job_pool):
        self.bus = bus
        self.deferred_worker = deferred_worker
        self.job_pool = job_pool
        self.idle_worker_queue = Queue()
        self.workers = {}
        self.netlocs = {}
        self.idle_set = set()
        self._lock = threading.RLock()
        self.feature_queues = FeatureQueues()
        self.event_count = 0
        self.event_condvar = threading.Condition(self._lock)
        self.max_concurrent_waiters = 5
        self.current_waiters = 0
        self.is_stopping = False
        self.scheduling_class_capacities = {'*' : []}
        self.scheduling_class_total_capacities = {'*' : 0}

    def subscribe(self):
        self.bus.subscribe('start', self.start, 75)
        self.bus.subscribe('stop', self.server_stopping, 10) 
        
    def unsubscribe(self):
        self.bus.unsubscribe('start', self.start, 75)
        self.bus.unsubscribe('stop', self.server_stopping) 

    def start(self):
        self.deferred_worker.do_deferred_after(30.0, self.reap_dead_workers)
        
    def reset(self):
        self.idle_worker_queue = Queue()
        self.workers = {}
        self.netlocs = {}
        self.idle_set = set()
        self.feature_queues = FeatureQueues()
        
    def allocate_worker_id(self):
        return str(uuid.uuid1())
        
    def create_worker(self, worker_descriptor):
        with self._lock:
            id = self.allocate_worker_id()
            worker = Worker(id, worker_descriptor, self.feature_queues, self)
            ciel.log.error('Worker registered: %s (%s)' % (worker.id, worker.netloc), 'WORKER_POOL', logging.WARNING, True)
            self.workers[id] = worker
            try:
                previous_worker_at_netloc = self.netlocs[worker.netloc]
                ciel.log.error('Worker at netloc %s has reappeared' % worker.netloc, 'WORKER_POOL', logging.WARNING)
                self.worker_failed(previous_worker_at_netloc)
            except KeyError:
                pass
            self.netlocs[worker.netloc] = worker
            #print "worker ", worker.netloc
            while True:
                try:
                    print "try create connection", worker.netloc
                    worker.conn = socket.create_connection((worker.netloc.split(":")[0], 8139))
                    worker.conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    RecvThread(worker, self.job_pool).start()
                    break
                except Exception as e:
                    print e
            print "created connection"
            self.idle_set.add(id)
            self.event_count += 1
            self.event_condvar.notify_all()
            
            for scheduling_class, capacity in worker.scheduling_classes.items():
                try:
                    capacities = self.scheduling_class_capacities[scheduling_class]
                    current_total = self.scheduling_class_total_capacities[scheduling_class]
                except:
                    capacities = []
                    self.scheduling_class_capacities[scheduling_class] = capacities
                    current_total = 0
                capacities.append((worker, capacity))
                self.scheduling_class_total_capacities[scheduling_class] = current_total + capacity

            self.job_pool.notify_worker_added(worker)
            return id

    def notify_job_about_current_workers(self, job):
        """Nasty function included to avoid the race between job creation and worker creation."""
        with self._lock:
            for worker in self.workers.values():
                job.notify_worker_added(worker)

# XXX: This is currently disabled because we don't have a big central list of references.
#        try:
#            has_blocks = worker_descriptor['has_blocks']
#        except:
#            has_blocks = False
#            
#        if has_blocks:
#            ciel.log.error('%s has blocks, so will fetch' % str(worker), 'WORKER_POOL', logging.INFO)
#            self.bus.publish('fetch_block_list', worker)
            
        self.bus.publish('schedule')
        return id
    
    def shutdown(self):
        for worker in self.workers.values():
            try:
                get_string('http://%s/control/kill/' % worker.netloc)
            except:
                pass
        
    def get_worker_by_id(self, id):
        with self._lock:
            return self.workers[id]
        
    def get_all_workers(self):
        with self._lock:
            return self.workers.values()
    
    def execute_task_on_worker(self, worker, task):
        try:
            ciel.stopwatch.stop("master_task")
            
            #print "sending yobits"
            #worker.conn.write("yobits")
            #print "task", task.as_descriptor()
            #message = simplejson.dumps(task.as_descriptor(), cls=SWReferenceJSONEncoder)
            #d = task.as_descriptor()
            #for k in d:
            #print k, d[k]
            #print len(task.as_protobuf()), len(message)
            #message = cPickle.dumps(task.as_descriptor())
            pb = task_pb2.Task()
            print pb
            task.fill_protobuf(pb)
            message = pb.SerializeToString()
            worker.conn.sendall(struct.pack("!I", len(message)) + message)
            #print "about to dump task"
            #cPickle.dump(task.as_descriptor(), worker.conn)
            #worker.conn.flush()
            #print "dumped task"
            #post_string_noreturn("http://%s/control/task/" % (worker.netloc), message, result_callback=self.worker_post_result_callback)
        except:
            self.worker_failed(worker)

    def abort_task_on_worker(self, task, worker):
        try:
            ciel.log("Aborting task %s on worker %s" % (task.task_id, worker), "WORKER_POOL", logging.WARNING)
            post_string_noreturn('http://%s/control/abort/%s/%s' % (worker.netloc, task.job.id, task.task_id), "", result_callback=self.worker_post_result_callback)
        except:
            self.worker_failed(worker)
    
    def worker_failed(self, worker):
        ciel.log.error('Worker failed: %s (%s)' % (worker.id, worker.netloc), 'WORKER_POOL', logging.WARNING, True)
        with self._lock:
            worker.failed = True
            del self.netlocs[worker.netloc]
            del self.workers[worker.id]

            for scheduling_class, capacity in worker.scheduling_classes.items():
                self.scheduling_class_capacities[scheduling_class].remove((worker, capacity))
                self.scheduling_class_total_capacities[scheduling_class] -= capacity
                if self.scheduling_class_total_capacities[scheduling_class] == 0:
                    del self.scheduling_class_capacities[scheduling_class]
                    del self.scheduling_class_total_capacities[scheduling_class]

        if self.job_pool is not None:
            self.job_pool.notify_worker_failed(worker)

    def worker_ping(self, worker):
        with self._lock:
            self.event_count += 1
            self.event_condvar.notify_all()
        worker.last_ping = datetime.datetime.now()

    def server_stopping(self):
        with self._lock:
            self.is_stopping = True
            self.event_condvar.notify_all()

    def investigate_worker_failure(self, worker):
        ciel.log.error('Investigating possible failure of worker %s (%s)' % (worker.id, worker.netloc), 'WORKER_POOL', logging.WARNING)
        try:
            content = get_string('http://%s/control/master/' % worker.netloc)
            worker_fetch = simplejson.loads(content)
            assert worker_fetch['id'] == worker.id
        except:
            self.worker_failed(worker)

    def get_random_worker(self):
        with self._lock:
            return random.choice(self.workers.values())
        
    def get_random_worker_with_capacity_weight(self, scheduling_class):
        
        with self._lock:
            try:
                candidates = self.scheduling_class_capacities[scheduling_class]
                total_capacity = self.scheduling_class_total_capacities[scheduling_class]
            except KeyError:
                scheduling_class = '*'
                candidates = self.scheduling_class_capacities['*']
                total_capacity = self.scheduling_class_total_capacities['*']
        
            if total_capacity == 0:
                return None

            selected_slot = random.randrange(total_capacity)
            curr_slot = 0
            i = 0
            
            for worker, capacity in candidates:
                curr_slot += capacity
                if curr_slot > selected_slot:
                    return worker
            
            ciel.log('Ran out of workers in capacity-weighted selection class=%s selected=%d total=%d' % (scheduling_class, selected_slot, total_capacity), 'WORKER_POOL', logging.ERROR)
            
    def get_worker_at_netloc(self, netloc):
        try:
            return self.netlocs[netloc]
        except KeyError:
            return None

    def reap_dead_workers(self):
        if not self.is_stopping:
            for worker in self.workers.values():
                if worker.failed:
                    continue
                if (worker.last_ping + datetime.timedelta(seconds=10)) < datetime.datetime.now():
                    failed_worker = worker
                    self.deferred_worker.do_deferred(lambda: self.investigate_worker_failure(failed_worker))
                    
            self.deferred_worker.do_deferred_after(10.0, self.reap_dead_workers)

    def worker_post_result_callback(self, success, url):
        # An asynchronous post_string_noreturn has completed against 'url'. Called from the cURL thread.
        if not success:
            parsed = urlparse(url)
            worker = self.get_worker_at_netloc(parsed.netloc)
            if worker is not None:
                ciel.log("Aysnchronous post against %s failed: investigating" % url, "WORKER_POOL", logging.ERROR)
                # Safe to call from here: this bottoms out in a deferred-work call quickly.
                self.worker_failed(worker)
            else:
                ciel.log("Asynchronous post against %s failed, but we have no matching worker. Ignored." % url, "WORKER_POOL", logging.WARNING)

