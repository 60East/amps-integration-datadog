from checks import AgentCheck
import requests
import time
import functools
import operator


def filter_on(key, value, op, vector):
    return filter(lambda k: op(k[key], value), vector)[0]


def extractor(key, vector):
    return map(lambda r: r[key], vector)


def sub_select(key, value, op=operator.eq):
    return functools.partial(filter_on, key, value, op)


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
    def add_count(self, stats_collection, prefix, property_name, type_func, tags):
        if stats_collection.has_key(property_name):
            self.count("%s.%s" % (prefix, property_name), type_func(stats_collection[property_name]), tags=tags)

    def add_counts(self, stats_collection, prefix, property_type_pairs, tags):
        for property_name, type_func in property_type_pairs:
            self.add_count(stats_collection, prefix, property_name, type_func, tags)

    def check(self, instance):
        if 'admin' not in instance:
            self.log.info("Skipping instance, no admin found.")
            return
        if 'name' not in instance:
            self.log.info("Defaulting name to admin location.")
            instance['name'] = instance['admin']

        # Load values from the instance config
        admin = instance['admin']
        instance_tags = instance.get('tags', [])
        default_timeout = self.init_config.get('default_timeout', 5)
        timeout = float(instance.get('timeout', default_timeout))

        start_time = time.time()
        try:
            r = requests.get("http://%s/amps/instance.json" % admin, timeout=timeout)
            document = r.json()

            self.gauge('amps.admin.response_time', time.time() - start_time)

            # Client load metrics
            for client in fetch(document, "amps", "instance", "clients"):
                tags = instance_tags + ["client:%s" % client["client_name"]]
                properties = [("bytes_in_per_sec", float),
                              ("bytes_out_per_sec", float),
                              ("denied_writes", int),
                              ("denied_reads", int),
                              ("messages_in_per_sec", int),
                              ("messages_out_per_sec", int),
                              ("query_time", float),
                              ("query_depth_out", int),
                              ("queue_max_latency", float),
                              ("subscription_count", int),
                              ("slow", float),
                              ("transport_rx_queue", int),
                              ("transport_tx_queue", int)]
                self.add_counts(client, 'amps.client', properties, tags)

            self.count('amps.clients.total', fetch(document, "amps", "instance", "clients", len), tags=instance_tags)
            self.count('amps.subscriptions.total', fetch(document, "amps", "instance", "subscriptions", len), tags=instance_tags)

            # Instance Memory
            self.gauge('amps.memory.vmsize', fetch(document, "amps", "instance", "memory", "vmsize", int), tags=instance_tags)
            self.gauge('amps.memory.rss', fetch(document, "amps", "instance", "memory", "rss", int), tags=instance_tags)

            # Event queues
            self.count('amps.queries.queued', fetch(document, "amps", "instance", "queries", "queued_queries", int), tags=instance_tags)
            self.count('amps.api.command_queue_depth', fetch(document, "amps", "instance", "api", "command_queue_depth", int), tags=instance_tags)

            # Messaging Rates
            self.gauge('amps.processing.messages_received_per_sec', fetch(document, "amps", "instance", "processors", sub_select("id", "all"), "messages_received_per_sec", float), tags=instance_tags)
            self.gauge('amps.processing.matches_found_per_sec', fetch(document, "amps", "instance", "processors", sub_select("id", "all"), "matches_found_per_sec", float), tags=instance_tags)

            # Entitlement denials
            self.count('amps.processing.denied_reads', fetch(document, "amps", "instance", "processors", sub_select("id", "all"), "denied_reads", int), tags=instance_tags)
            self.count('amps.processing.denied_writes', fetch(document, "amps", "instance", "processors", sub_select("id", "all"), "denied_writes", int), tags=instance_tags)

            # SOW Metrics
            for topic in fetch(document, "amps", "instance", "sow"):
                tags = instance_tags + ["topic:%s" % topic["topic"]]
                properties = [("valid_keys", int),
                              ("deletes_per_sec", int),
                              ("inserts_per_sec", int),
                              ("updates_per_sec", int),
                              ("queries_per_sec", int),
                              ("query_count", int),
                              ("stored_bytes", int),
                              ("memory_bytes", int)]
                self.add_counts(topic, 'amps.sow', properties, tags)

            # Queue Metrics
            for topic in fetch(document, "amps", "instance", "queues"):
                tags = instance_tags + ["queue:%s" % topic["topic"]]
                properties = [("age_of_oldest_lease", float),
                              ("backlog", int),
                              ("expired_leases", int),
                              ("owned", int),
                              ("queue_depth", int),
                              ("seconds_behind", float),
                              ("transfered_in", int),
                              ("transfered_out", int)]
                self.add_counts(topic, 'amps.queue', properties, tags)

            # View Metrics
            for topic in fetch(document, "amps", "instance", "views"):
                tags = instance_tags + ["view:%s" % topic["topic"]]
                properties = [("conflation_ratio", float),
                              ("queue_depth", int)]
                self.add_counts(topic, 'amps.view', properties, tags)

            # Check the health of the message processors
            self.count('amps.processors.throttle_count', fetch(document, "amps", "instance", "processors", sub_select("id", "all"), "throttle_count", int), tags=instance_tags)
            processor_last_active = fetch(document, "amps", "instance", "processors", sub_select("id", "all"), "last_active", float)
            self.gauge('amps.processors.last_active', processor_last_active, tags=instance_tags)

        except requests.exceptions.Timeout as e:
            # If there's a timeout
            msg = "Connection to AMPS admin timed out"
            self.service_check('amps.admin.check', AgentCheck.WARNING, message=msg)
        except requests.exceptions.ConnectionError, e:
            # AMPS instance is down
            msg = "Connection to AMPS transport refused. Service may not be running"
            self.service_check('amps.admin.check', AgentCheck.CRITICAL, message=msg)
        except Exception, e:
            # AMPS instance is down?
            msg = "Connection to AMPS transport refused. Service may not be running"
            self.service_check('amps.admin.check', AgentCheck.CRITICAL, message=msg)


if __name__ == '__main__':
    check, instances = AMPSCheck.from_yaml('/etc/dd-agent/conf.d/amps.yaml')
    for instance in instances:
        print('\nRunning the check against AMPS @ {0}'.format(instance['admin']))
        check.check(instance)
        if check.has_events():
            print('Events: {0}'.format(check.get_events()))
        print('Metrics: {0}'.format(check.get_metrics()))
