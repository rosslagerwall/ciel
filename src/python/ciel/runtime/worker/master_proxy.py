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
import ciel

'''
Created on 15 Apr 2010

@author: dgm36
'''
from urlparse import urljoin
from ciel.public.references import SWReferenceJSONEncoder
from ciel.runtime.exceptions import MasterNotRespondingException,\
    WorkerShutdownException
import logging
import random
import cherrypy
import socket
import httplib2
from ciel.runtime.pycurl_rpc import post_string, post_string_noreturn, get_string
from threading import Event
import struct

import json

def get_worker_netloc():
    return '%s:%d' % (socket.getfqdn(), cherrypy.config.get('server.socket_port'))

class MasterProxy:
    
    def __init__(self, worker, bus, master_url=None):
        self.bus = bus
        self.worker = worker
        self.master_url = master_url
        self.stop_event = Event()

    def subscribe(self):
        # Stopping is high-priority
        self.bus.subscribe("stop", self.handle_shutdown, 10)

    def unsubscribe(self):
        self.bus.unsubscribe("stop", self.handle_shutdown)

    def change_master(self, master_url):
        self.master_url = master_url
        
    def get_master_details(self):
        return {'netloc': self.master_url, 'id':str(self.worker.id)}

    def handle_shutdown(self):
        self.stop_event.set()
    
    def backoff_request(self, url, method, payload=None, need_result=True, callback=None):
        if self.stop_event.is_set():
            return
        try:
            if method == "POST":
                if need_result:
                    content = post_string(url, payload)
                else:
                    if callback is None:
                        callback = self.master_post_result_callback
                    post_string_noreturn(url, payload, result_callback=callback)
                    return
            elif method == "GET":
                content = get_string(url)
            else:
                raise Exception("Invalid method %s" % method)
            return 200, content
        except:
            ciel.log("Error attempting to contact master, aborting", "MSTRPRXY", logging.WARNING, True)
            raise
    
    def _backoff_request(self, url, method, payload=None, num_attempts=1, initial_wait=0, need_result=True, callback=None):
        initial_wait = 5
        for _ in range(0, num_attempts):
            if self.stop_event.is_set():
                break
            try:
                try:
                    if method == "POST":
                        if need_result or num_attempts > 1:
                            content = post_string(url, payload)
                        else:
                            if callback is None:
                                callback = self.master_post_result_callback
                            post_string_noreturn(url, payload, result_callback=callback)
                            return
                    elif method == "GET":
                        content = get_string(url)
                    else:
                        raise Exception("Invalid method %s" % method)
                    return 200, content
                except Exception as e:
                    ciel.log("Backoff-request failed with exception %s; re-raising MasterNotResponding" % e, "MASTER_PROXY", logging.ERROR)
                    raise MasterNotRespondingException()
            except:
                ciel.log.error("Error contacting master", "MSTRPRXY", logging.WARN, True)
            self.stop_event.wait(initial_wait)
            initial_wait += initial_wait * random.uniform(0.5, 1.5)
        ciel.log.error("Given up trying to contact master", "MSTRPRXY", logging.ERROR, True)
        if self.stop_event.is_set():
            raise WorkerShutdownException()
        else:
            raise MasterNotRespondingException()

    def register_as_worker(self):
        message_payload = json.dumps(self.worker.as_descriptor())
        message_url = urljoin(self.master_url, 'control/worker/')
        _, result = self.backoff_request(message_url, 'POST', message_payload)
        self.worker.id = json.loads(result)
    
    def publish_refs(self, job_id, task_id, refs):
        message_payload = json.dumps(refs, cls=SWReferenceJSONEncoder)
        message_url = urljoin(self.master_url, 'control/task/%s/%s/publish' % (job_id, task_id))
        self.backoff_request(message_url, "POST", message_payload, need_result=False)

    def log(self, job_id, task_id, timestamp, message):
        message_payload = json.dumps([timestamp, message], cls=SWReferenceJSONEncoder)
        message_url = urljoin(self.master_url, 'control/task/%s/%s/log' % (job_id, task_id))
        self.backoff_request(message_url, "POST", message_payload, need_result=False)

    def report_tasks(self, job_id, root_task_id, report):
        #print "report"
        message_payload = job_id + '!' + root_task_id + '@' + json.dumps({'worker' : self.worker.id, 'report' : report}, cls=SWReferenceJSONEncoder)
        #print repr(struct.pack('i', len(message_payload)) + message_payload.encode("utf-8"))
        self.worker.conn.sendall(struct.pack('i', len(message_payload)) + message_payload.encode("utf-8"))
        #message_url = urljoin(self.master_url, 'control/task/%s/%s/report' % (job_id, root_task_id))
        #self.backoff_request(message_url, "POST", message_payload, need_result=False)

    def failed_task(self, job_id, task_id, reason=None, details=None, bindings={}):
        message_payload = json.dumps((reason, details, bindings), cls=SWReferenceJSONEncoder)
        message_url = urljoin(self.master_url, 'control/task/%s/%s/failed' % (job_id, task_id))
        self.backoff_request(message_url, "POST", message_payload, need_result=False)

    def get_public_hostname(self):
        message_url = urljoin(self.master_url, "control/gethostname/")
        _, result = self.backoff_request(message_url, "GET")
        return json.loads(result)

    def ping(self, ping_fail_callback):
        message_url = urljoin(self.master_url, 'control/worker/%s/ping/' % (str(self.worker.id), ))
        self.backoff_request(message_url, "POST", "PING", need_result=False, callback=ping_fail_callback)

    def master_post_result_callback(self, success, url):
        if not success:
            ciel.log("Failed to async-post to %s!" % url, "MASTER_PROXY", logging.ERROR)

