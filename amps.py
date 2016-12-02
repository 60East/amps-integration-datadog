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
            self.count('amps.clients.total', fetch(document, "amps", "instance", "clients", len))
            self.count('amps.clients.slow', fetch(document, "amps", "instance", "clients", F("queue_max_latency",slow_client_threshold,operator.gt), len))
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
            self.count('amps.sow.records', fetch(document, "amps", "instance", "sow", E("valid_keys"), M(int), sum))
            self.count('amps.sow.deletes_per_sec', fetch(document, "amps", "instance", "sow", E("deletes_per_sec"), M(int), sum))
            self.count('amps.sow.inserts_per_sec', fetch(document, "amps", "instance", "sow", E("inserts_per_sec"), M(int), sum))
            self.count('amps.sow.updates_per_sec', fetch(document, "amps", "instance", "sow", E("updates_per_sec"), M(int), sum))
            self.count('amps.sow.queries_per_sec', fetch(document, "amps", "instance", "sow", E("queries_per_sec"), M(int), sum))
            self.count('amps.sow.query_count', fetch(document, "amps", "instance", "sow", E("query_count"), M(int), sum))
            self.count('amps.sow.stored_bytes', fetch(document, "amps", "instance", "sow", E("stored_bytes"), M(int), sum))
            self.count('amps.sow.memory_bytes', fetch(document, "amps", "instance", "sow", E("memory_bytes"), M(int), sum))

            # Check the health of the message processors
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
