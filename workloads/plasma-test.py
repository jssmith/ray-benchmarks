#!/usr/bin/env python
from __future__ import print_function
import os
import plasma
import numpy as np
import time

import raybench
import raybench.eventstats as eventstats


#start the store : numactl --physcpubind=0 ./plasma_store -s "/tmp/plasma" -m 100000000000

def plasma_create_objects(numobj, size=500):
    oid_list = []
    for i in range(numobj):
      oid = np.random.bytes(20)
      oid_list.append(oid)
    
    t1 = time.time()
    for i in range(numobj): oid = oid_list[i]; client.create(oid, size); client.seal(oid);
    t2 = time.time()
    print("time to store numobj={} objects={}".format(numobj, t2-t1))
    return oid_list, t2-t1

def plasma_get_objects(oid_list):
    t1 = time.time()
    x = client.get(oid_list)
    t2 = time.time()
    print("time to get numobj={} objects={}".format(len(oid_list), t2-t1) )
    return t2-t1

def TEST_PutAfterPut(numobj):
    with eventstats.BenchmarkLogSpan("put_after_put", {"numobj" : numobj}):
        l1,dt1 = plasma_create_objects(numobj)
        time.sleep(1)
        l2,dt2 = plasma_create_objects(numobj)
        print("TEST_PutAfterPut: slowdown factor={}".format(dt2/dt1))
        return dt2/dt1

def TEST_PutLinearScale():
    for object_size in 10**3, 10**4, 10** 5, 10**6, 10**7:
        time.sleep(1)
        with eventstats.BenchmarkLogSpan("put_get_linear_scale", {"object_size" : object_size}):
            lst,dt = plasma_create_objects(object_size)

def TEST_PutGetLinearScale():
    for object_size in 10**3, 10**4, 10** 5, 10**6:
        time.sleep(1)
        with eventstats.BenchmarkLogSpan("put_get_linear_scale", {"object_size" : object_size}):
            l, pdt = plasma_create_objects(object_size)
            gdt1 = plasma_get_objects(l)

def TEST_GetBeforeAfterPut(numobj_getperf, numobj_put):
    with eventstats.BenchmarkLogSpan("get_before_after_put", {"numobj_getperf" : numobj_getperf, "numobj_put" : numobj_put}):
        lst1,putdt1 = plasma_create_objects(numobj_getperf)
        dt1 = plasma_get_objects(lst1)
        lst2,putdt2 = plasma_create_objects(numobj_put)
        #dt2 = plasma_get_objects(lst2) #known bug: this causes a crash
        #now try to match dt1 get time again
        dt2 = plasma_get_objects(lst1)
        lst3,putdt3 = plasma_create_objects(numobj_getperf)
        dt3 = plasma_get_objects(lst3)
        print("relative time={} to get initial numobj={} objects".format(dt2/dt1, numobj_getperf))
        print("relative time={} to get new numobj={} objects".format(dt3/dt1, numobj_getperf))

def get_plasma_client():
    for name in os.path.listdir("/tmp"):
        if (name.startswith("plasma_store")):
            return os.path.join("/tmp", name)

if __name__ == "__main__":
    bench_env = raybench.Env()
    out = bench_env.ray_init()
    plasma_socket = out['object_store_addresses'][0].name
    client = plasma.PlasmaClient(plasma_socket)
    TEST_PutAfterPut(10**6)
    #ray.worker.cleanup()
    #time.sleep(1)

    #out = ray.init()
    #plasma_socket = out['object_store_addresses'][0].name
    #client = plasma.PlasmaClient(plasma_socket)
    TEST_GetBeforeAfterPut(10**5, 10**6)
    TEST_PutGetLinearScale()
    TEST_PutLinearScale()

    print("BENCHMARK_STATS:", json.dumps({
        "config": { },
        "events" : eventstats.log_span_events() }))

    ray.flush_log()
