from checks import AgentCheck
from hashlib import md5
import urllib2, json, requests, time, socket, errno, functools, operator

def filter_on(key, value, op, vector):
    return filter(lambda k: op(k[key], value), vector)[0]

def extractor(key, vector):
    return map(lambda r: r[key], vector)

def F(key,value,op=operator.eq):
    return functools.partial(filter_on, key, value, op)

def E(key):
    return functools.partial(extractor, key)

def M(f):
    return functools.partial(map, lambda v: f(v))

def fetch(document, *path):
    try:
        if len(path) > 0:
            navigator = path[0]
            if type(navigator) is str:
                return fetch(document[navigator], *path[1:])
            else:
                return fetch(navigator(document), *path[1:])
        else:
            return document
    except:
        return None


class AMPSCheck(AgentCheck):

    def check(self, instance):
        if 'admin' not in instance:
            self.log.info("Skipping instance, no admin found.")
            return
        if 'name' not in instance:
            self.log.info("Defaulting name to admin location.")
            instance['name'] = instance['admin']

        # Load values from the instance config
        admin = instance['admin']
        name  = instance['name']
        default_timeout = self.init_config.get('default_timeout', 5)
        timeout = float(instance.get('timeout', default_timeout))
        slow_client_threshold = float(self.init_config.get('slow_client_threshold', 1.0))
        message_processor_activity_threshold = float(self.init_config.get('message_processor_activity_threshold', 20.0))

        # Use a hash of the URL as an aggregation key
        aggregation_key = md5(admin).hexdigest()
        
        start_time = time.time()
        try:
            r = requests.get("http://%s/amps/instance.json" % admin, timeout=timeout)
            document = r.json()

            self.gauge('amps.admin.reponse_time', time.time() - start_time)

            # Client load metrics
            for client in fetch(document, "amps", "instance", "clients"):
                tags = ["client:%s" % client["client_name"]]
                self.count('amps.client.bytes_in_per_sec', float(client["bytes_in_per_sec"]), tags=tags)
                self.count('amps.client.bytes_out_per_sec', float(client["bytes_out_per_sec"]), tags=tags)
                self.count('amps.client.denied_writes', int(client["denied_writes"]), tags=tags)
                self.count('amps.client.denied_reads', int(client["denied_reads"]), tags=tags)
                self.count('amps.client.messages_in_per_sec', int(client["messages_in_per_sec"]), tags=tags)
                self.count('amps.client.messages_out_per_sec', int(client["messages_out_per_sec"]), tags=tags)
                self.count('amps.client.query_time', float(client["query_time"]), tags=tags)
                self.count('amps.client.queue_depth_out', int(client["queue_depth_out"]), tags=tags)
                self.count('amps.client.queue_max_latency', float(client["queue_max_latency"]), tags=tags)
                self.count('amps.client.subscription_count', int(client["subscription_count"]), tags=tags)
                self.count('amps.client.slow', float(client["queue_max_latency"])>slow_client_threshold, tags=tags)
                # New metrics in 5.2
                if client.has_key("transport_rx_queue"):
                    self.count('amps.client.transport_rx_queue', int(client["transport_rx_queue"]), tags=tags)
                    self.count('amps.client.transport_tx_queue', int(client["transport_tx_queue"]), tags=tags)


            self.count('amps.clients.total', fetch(document, "amps", "instance", "clients", len))
            self.count('amps.subscriptions.total', fetch(document, "amps", "instance", "subscriptions", len))

            # Instance Memory
            self.gauge('amps.memory.vmsize', fetch(document, "amps", "instance", "memory", "vmsize", int))
            self.gauge('amps.memory.rss', fetch(document, "amps", "instance", "memory", "rss", int))

            # Event queues
            self.count('amps.queries.queued', fetch(document, "amps", "instance", "queries", "queued_queries", int))
            self.count('amps.views.queue_depth', fetch(document, "amps", "instance", "views", E("queue_depth"), M(int), sum))
            self.count('amps.api.command_queue_depth', fetch(document, "amps", "instance", "api", "command_queue_depth", int))

            # Messaging Rates
            self.gauge('amps.processing.messages_received_per_sec', fetch(document, "amps", "instance", "processors", F("id","all"), "messages_received_per_sec", float))
            self.gauge('amps.processing.matches_found_per_sec', fetch(document, "amps", "instance", "processors", F("id","all"), "matches_found_per_sec", float))

            # Entitlement denials
            self.count('amps.processing.denied_reads', fetch(document, "amps", "instance", "processors", F("id","all"), "denied_reads", int))
            self.count('amps.processing.denied_writes', fetch(document, "amps", "instance", "processors", F("id","all"), "denied_writes", int))

            # SOW Metrics
            for topic in fetch(document, "amps", "instance", "sow"):
                tags = ["topic:%s" % topic["topic"]]
                self.count('amps.sow.records', int(topic["valid_keys"]), tags=tags)
                self.count('amps.sow.deletes_per_sec', int(topic["deletes_per_sec"]), tags=tags)
                self.count('amps.sow.inserts_per_sec', int(topic["inserts_per_sec"]), tags=tags)
                self.count('amps.sow.updates_per_sec', int(topic["updates_per_sec"]), tags=tags)
                self.count('amps.sow.queries_per_sec', int(topic["queries_per_sec"]), tags=tags)
                self.count('amps.sow.query_count', int(topic["query_count"]), tags=tags)
                self.count('amps.sow.stored_bytes', int(topic["stored_bytes"]), tags=tags)
                self.count('amps.sow.memory_bytes', int(topic["memory_bytes"]), tags=tags)

            # Queue Metrics
            for topic in fetch(document, "amps", "instance", "queues"):
                tags = ["queue:%s" % topic["topic"]]
                self.count('amps.queue.age_of_oldest_lease', float(topic["age_of_oldest_lease"]), tags=tags)
                self.count('amps.queue.backlog', int(topic["backlog"]), tags=tags)
                self.count('amps.queue.expired_leases', int(topic["expired_leases"]), tags=tags)
                self.count('amps.queue.owned', int(topic["owned"]), tags=tags)
                self.count('amps.queue.queue_depth', int(topic["queue_depth"]), tags=tags)
                self.count('amps.queue.seconds_behind', float(topic["seconds_behind"]), tags=tags)
                self.count('amps.queue.transferred_in', int(topic["transferred_in"]), tags=tags)
                self.count('amps.queue.transferred_out', int(topic["transferred_out"]), tags=tags)

            # View Metrics
            for topic in fetch(document, "amps", "instance", "views"):
                tags = ["view:%s" % topic["topic"]]
                self.count('amps.view.conflation_ratio', float(topic["conflation_ratio"]), tags=tags)
                self.count('amps.view.queue_depth', int(topic["conflation_ratio"]), tags=tags)

            # Check the health of the message processors
            self.count('amps.processors.throttle_count', fetch(document, "amps", "instance", "processors", F("id","all"), "throttle_count", int))
            processor_last_active = fetch(document, "amps", "instance", "processors", F("id","all"), "last_active", float)
            if processor_last_active > 1000.0*message_processor_activity_threshold:
                self.service_check('amps.processing.check', AgentCheck.CRITICAL, timestamp=time.time())
            else:
                self.service_check('amps.processing.check', AgentCheck.OK, timestamp=time.time())

        except requests.exceptions.Timeout as e:
            # If there's a timeout
            self.service_check('amps.admin.check', AgentCheck.WARNING, timestamp=time.time())
        except requests.exceptions.ConnectionError, e:
            # AMPS instance is down
            self.service_check('amps.admin.check', AgentCheck.CRITICAL, timestamp=time.time())
        except Exception, e:
            # AMPS instance is down?
            self.service_check('amps.admin.check', AgentCheck.CRITICAL, timestamp=time.time())


if __name__ == '__main__':
    check, instances = AMPSCheck.from_yaml('/etc/dd-agent/conf.d/amps.yaml')
    for instance in instances:
        print "\nRunning the check against AMPS @ %s" % (instance['admin'])
        check.check(instance)
        if check.has_events():
            print 'Events: %s' % (check.get_events())
        print 'Metrics: %s' % (check.get_metrics())
