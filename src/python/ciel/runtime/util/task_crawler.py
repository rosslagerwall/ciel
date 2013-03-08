# Copyright (c) 2010 Derek Murray <Derek.Murray@cl.cam.ac.uk>
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

from Queue import Queue, Empty
from ciel.public.references import json_decode_object_hook
from urlparse import urljoin, urlparse
import httplib2
import json
import sys

def sanitise_job_url(root_url):

    h = httplib2.Http()

    # Postel's Law!
    # We expect the URL of a root task; however, we should liberally accept
    # URLs starting with '/control/browse/job/', '/control/job/' and '/control/browse/task/', and URLs missing '/control'.
    url_parts = urlparse(root_url)

    if not url_parts.path.startswith('/control'):
        root_url = urljoin(root_url, '/control' + url_parts.path)
        url_parts = urlparse(root_url)

    if url_parts.path.startswith('/control/browse/'):
        root_url = urljoin(root_url, '/control' + url_parts.path[len('/control/browse'):])
        url_parts = urlparse(root_url)

    if url_parts.path.startswith('/control/job/'):
        job_url = root_url
        _, content = h.request(job_url)
        job_descriptor = json.loads(content)
        root_url = urljoin(root_url, '/control/task/%s/%s' % (job_descriptor['job_id'], job_descriptor['root_task']))
    elif not url_parts.path.startswith('/control/task/'):
        print >>sys.stderr, "Error: must specify task or job URL."
        raise Exception()
        
    return root_url

def task_descriptors_for_job(job_url, sanitise=True):

    h = httplib2.Http()
    q = Queue()
    q.put(sanitise_job_url(job_url) if sanitise else job_url)

    while True:
        try:
            url = q.get(block=False)
        except Empty:
            break
        _, content = h.request(url)
        
        descriptor = json.loads(content, object_hook=json_decode_object_hook)

        for child in descriptor["children"]:
            q.put(urljoin(url, child))
            
        yield descriptor

def main():
    
    root_url = sys.argv[1]
        
    print 'task_id type parent created_at assigned_at committed_at duration num_children num_dependencies num_outputs final_state worker total_bytes_fetched'
    for descriptor in task_descriptors_for_job(root_url):

        try:
            total_bytes_fetched = sum(descriptor["profiling"]["FETCHED"].values())
        except:
            total_bytes_fetched = None

        task_id = descriptor["task_id"]
        parent = descriptor["parent"]

        #try:
        #    worker = descriptor["worker"] 
        #except KeyError:
        #    worker = None

        created_at = None
        assigned_at = None
        committed_at = None

        num_children = len(descriptor["children"])

        num_dependencies = len(descriptor["dependencies"])

        num_outputs = len(descriptor["expected_outputs"])

        type = descriptor["handler"]

        final_state = descriptor["state"]

        worker = None
        duration = None

        for (time, state) in descriptor["history"]:
            #print time, state
            if state == 'CREATED':
                created_at = time
            elif state == 'COMMITTED':
                committed_at = time
                duration = committed_at - assigned_at if (committed_at is not None and assigned_at is not None) else None
                print task_id, type, parent, created_at, assigned_at, committed_at, duration, num_children, num_dependencies, num_outputs, 'COMMITTED', worker, total_bytes_fetched
            elif state == 'FAILED':
                committed_at = time
                duration = committed_at - assigned_at if (committed_at is not None and assigned_at is not None) else None
                print task_id, type, parent, created_at, assigned_at, committed_at, duration, num_children, num_dependencies, num_outputs, 'FAILED', worker, total_bytes_fetched
            else:
                try:
                    if state[0] == 'ASSIGNED':
                        assigned_at = time
                        worker = state[1]
                        assigned_at = time
                        committed_at = None
                        duration = None
                except ValueError:
                    pass

        if committed_at is None:
            print task_id, type, parent, created_at, assigned_at, committed_at, duration, num_children, num_dependencies, num_outputs, final_state, worker, total_bytes_fetched

        
        #print task_id, type, parent, created_at, assigned_at, committed_at, duration, num_children, num_dependencies, num_outputs, final_state, worker, total_bytes_fetched


            
if __name__ == '__main__':
    main()
