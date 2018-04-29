#!/usr/bin/env python

import sys
import time
import json
from kafka import KafkaProducer

from bluepy.btle import BTLEException

from blue_st_sdk.manager import Manager
from blue_st_sdk.manager import ManagerListener
from blue_st_sdk.node import NodeListener
from blue_st_sdk.feature import FeatureListener

# Bluetooth Scanning time in seconds.
SCANNING_TIME_s = 5

kafka_producer = KafkaProducer(bootstrap_servers='hdp01.woolford.io:6667')


class MyManagerListener(ManagerListener):

    #
    # This method is called whenever a discovery process starts or stops.
    #
    # @param manager Manager instance that starts/stops the process.
    # @param enabled True if a new discovery starts, False otherwise.
    #
    def on_discovery_change(self, manager, enabled):
        print('Discovery %s.' % ('started' if enabled else 'stopped'))
        if not enabled:
            print()

    #
    # This method is called whenever a new node is discovered.
    #
    # @param manager Manager instance that discovers the node.
    # @param node    New node discovered.
    #
    def on_node_discovered(self, manager, node):
        print('New device discovered: %s.' % (node.get_name()))


class MyNodeListener(NodeListener):

    #
    # To be called whenever a node changes its status.
    #
    # @param node       Node that has changed its status.
    # @param new_status New node status.
    # @param old_status Old node status.
    #
    def on_status_change(self, node, new_status, old_status):
        print('Device %s went from %s to %s.' %
            (node.get_name(), str(old_status), str(new_status)))


class MyFeatureListener(FeatureListener):

    #
    # To be called whenever the feature updates its data.
    #
    # @param feature Feature that has updated.
    # @param sample  Data extracted from the feature.
    #
    def on_update(self, feature, sample):
        sample_keys = [elem._name for elem in sample.get_description()]
        sample_values = [elem for elem in sample.get_data()]
        record = dict(zip(sample_keys, sample_values))
        record['type'] = feature.get_name()
        record['timestamp'] = time.mktime(sample.get_notification_time().timetuple())
        record['device_name'] = feature.get_parent_node().get_name()
        record['device_addr'] = feature.get_parent_node().addr
        kafka_producer.send('sensortile', json.dumps(record))


def main():
    try:
        # Creating Bluetooth Manager.
        manager = Manager.instance()
        manager_listener = MyManagerListener()
        manager.add_listener(manager_listener)

        while True:
            # Synchronous discovery of Bluetooth devices.
            print('Scanning Bluetooth devices...\n')
            manager.discover(False, SCANNING_TIME_s)

            for device in manager.get_nodes():

                # Connecting to the device.
                node_listener = MyNodeListener()
                device.add_listener(node_listener)
                print('\nConnecting to %s...' % (device.get_name()))
                device.connect()

                features = device.get_features()

                for feature in features:
                    feature_listener = MyFeatureListener()
                    feature.add_listener(feature_listener)
                    device.enable_notifications(feature)

    except BTLEException as e:
        print(e)
        # Exiting.
        print('Exiting...\n')
        sys.exit(0)


if __name__ == "__main__":
    main()
