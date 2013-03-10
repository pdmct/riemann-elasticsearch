#!/usr/bin/python

# an elasticsearch client for riemann
# 
#
import errno
import httplib
import json
import socket
import sys
import time

# uses the bernhard riemann python client 
# see https://github.com/banjiewen/bernhard
import bernhard

INTERVAL_SEC = 15  			# seconds
DEFAULT_TIMEOUT = 10.0    	# seconds
DEFAULT_HOST = "localhost"  # run on the same host
DEFAULT_PORT = 9200         # default ES port

STATE_MAP = {
  "green": 0,
  "yellow": 1,
  "red": 2,
}

def is_numeric(value):
  return isinstance(value, (int, long, float))


def err(msg):
  print >>sys.stderr, msg


class HTTPError(RuntimeError):
  """deal with HTTP error response"""

  def __init__(self, resp):
    RuntimeError.__init__(self, str(resp))
    self.resp = resp


def request(server, uri):
  """Does a GET request of the given uri on the given HTTPConnection."""
  server.request("GET", uri)
  resp = server.getresponse()
  if resp.status != httplib.OK:
    raise ESError(resp)
  return json.loads(resp.read())


def clusterHealth(server):
  return request(server, "/_cluster/health")


def clusterState(server):
  return request(server, "/_cluster/state"
                 + "?filter_routing_table=true&filter_metadata=true&filter_blocks=true")


def nodeStats(server):
  return request(server, "/_cluster/nodes/_local/stats")

def main(argv):
  # process command line arguments
  # expecting
  # es_host 
  # es_port
  # riemann_host
  # riemann_port
  # timeout
  # sample_interval
  timeout = DEFAULT_TIMEOUT
  sample_interval = INTERVAL_SEC
  es_host = DEFAULT_HOST
  es_port = DEFAULT_PORT


  ts = None
  socket.setdefaulttimeout(timeout)
  server = httplib.HTTPConnection(es_host, es_port)
  

  try:
    server.connect()
  except socket.error, (erno, e):
  	err("Cannot connect to Elasticsearch")
    return 1    
  if json is None:
    err("This collector requires the `json' Python module.")
    return 1

  # collect initial stats
  nodestats = nodeStats(server)
  cluster_name = nodestats["cluster_name"]
  nodeid, nodestats = nodestats["nodes"].popitem()

  # create Riemann client
  # c = bernhard.Client(riemann_host, riemann_port)
  c = bernhard.Client("10.73.67.145")

  def emitMetric(host, service, timestamp, metric, **tags):
    if tags:
      tags = " " + ",".join("%s" % (value)
                            for name, value in tags.iteritems())
    else:
      tags = ""

    hst = host
    svc = "elasticsearch."+ service
    mtrc = metric
    time = timestamp
    tgs =  list(cluster_name + ', ' + tags.split(","))
    
    c.send({  'host': hst, 
    		  'state':state, 
    		  'service': 'ok', 
    		  'time':time, 
    		  'metric': mtrc, 
    		  'description': '' + cluster_name + ' ' + str(tgs), 
    		  'tags': tgs})
    
 while True:
    ts = int(time.time())
    nodestats = nodeStats(server)
    
    # Check that the node's identity hasn't changed in the mean time.
    if nodestats["cluster_name"] != cluster_name:
      err("cluster_name changed from %r to %r"
          % (cluster_name, nodestats["cluster_name"]))
      return 1
    this_nodeid, nodestats = nodestats["nodes"].popitem()
    if this_nodeid != nodeid:
      err("node ID changed from %r to %r" % (nodeid, this_nodeid))
      return 1

    is_master = nodeid == cluster_state(server)["master_node"]
    emitMetrics(es_host,"is_master",ts,  int(is_master))
    if is_master:
      ts = int(time.time())  
      cstats = cluster_health(server)
      for stat, value in cstats.iteritems():
        if stat == "status":
          value = STATE_MAP.get(value, -1)
        elif not is_numeric(value):
          continue
        emitMetrics(es_host,"cluster." + stat, ts,  value)

    
    ts = nodestats["timestamp"] / 1000  # in seconds
    indices = nodestats["indices"]
    emitMetrics(es_host,"indices.size", ts, indices["store"]["size_in_bytes"] / (1024 * 1024)) # in MBytes
    emitMetrics(es_host,"num_docs", ts, indices["docs"]["count"])
    cache = indices["cache"]
    emitMetrics(es_host,"cache.field.evictions",ts,  cache["field_evictions"])
    emitMetrics(es_host,"cache.field.size", ts, cache["field_size_in_bytes"])
    emitMetrics(es_host,"cache.filter.count", ts, cache["filter_count"])
    emitMetrics(es_host,"cache.filter.evictions",ts,  cache["filter_evictions"])
    emitMetrics(es_host,"cache.filter.size",ts,  cache["filter_size_in_bytes"])
    merges = indices["merges"]
    emitMetrics(es_host,"merges.current", ts, merges["current"])
    emitMetrics(es_host,"merges.total", ts, merges["total"])
    emitMetrics(es_host,"merges.total_time", ts, merges["total_time_in_millis"] / 1000.) # seconds
    search = indices["search"]
    emitMetrics(es_host,"search.query_total", ts, search["query_total"])
    emitMetrics(es_host,"search.fetch_time_in_millis", ts, search["fetch_time_in_millis"])
    emitMetrics(es_host,"search.fetch_total", ts, search["fetch_total"])
    emitMetrics(es_host,"search.query_time_in_millis", ts, search["query_time_in_millis"])
    flush = indices["flush"]
    emitMetrics(es_host,"flush.total_time_in_millis", ts, flush["total_time_in_millis"])
    indexing = indices["indexing"]
    emitMetrics(es_host,"indexing.index_total", ts, indexing["index_total"])
    emitMetrics(es_host,"indexing.index_time_in_millis", ts, indexing["index_time_in_millis"])
    store = indices["store"]
    emitMetrics(es_host,"store.size_in_bytes", ts, store["size_in_bytes"])                
    emitMetrics(es_host,"store.throttle_time_in_millis", ts, store["throttle_time_in_millis"])                
                    
    del indices
    del nodestats

    time.sleep(sample_interval)


if __name__ == "__main__":
  sys.exit(main(sys.argv))


