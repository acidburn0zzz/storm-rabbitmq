package com.accelerate_experience.storm.rabbitmq.config;

import com.rabbitmq.client.Address;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Simple configuration class to allow users to specify a set hosts to connect to for high availability purposes.
 */
public class ConfigAvailableHosts implements Serializable {

    private static final long serialVersionUID = -7444594758428554141L;

    private static final String HOST_DELIMINITER = "|";

    /**
     * {@link TreeMap} of host configurations (order may matter...).
     */
    private final Map<String, Integer> hostsMap = new TreeMap<String, Integer>();

    /**
     * The map of hostname to port we'll be using to connect to.
     *
     * @return The map of hostname to port we'll use to connect to
     */
    public Map<String, Integer> getHostsMap() {
        return hostsMap;
    }

    /**
     * Is this configured for high availability.
     *
     * @return Whether or not there are any configured high availability hosts
     *         in this config
     */
    public boolean isEmpty() {
        return hostsMap.isEmpty();
    }

    /**
     * The map of RabbitMQ hosts.
     *
     * @return The {@link Map} of RabbitMQ hosts, converted to the necessary
     *         {@link Address} array
     */
    public Address[] toAddresses() {
        final Address[] addresses = new Address[hostsMap.size()];
        int addIter = 0;
        for (final Entry<String, Integer> entry : hostsMap.entrySet()) {
            if (entry.getKey() != null) {
                addresses[addIter++] = entry.getValue() == null ? new Address(entry.getKey()) :
                    new Address(entry.getKey(), entry.getValue());
            }
        }
        return addresses;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (final String host : hostsMap.keySet()) {
            final Integer port = hostsMap.get(host);
            if (!first) {
                builder.append(HOST_DELIMINITER);
            } else {
                first = false;
            }
            builder.append(host);
            builder.append(port != null ? ":" + port : "");
        }
        return builder.toString();
    }

    public static ConfigAvailableHosts fromString(final String serialzed) {
        final ConfigAvailableHosts value = new ConfigAvailableHosts();
        final String[] hosts = serialzed.split("[" + HOST_DELIMINITER + "]");
        for (final String host : hosts) {
            if (!host.isEmpty()) {
                String[] brokenUp = host.split(":");
                value.getHostsMap().put(brokenUp[0], brokenUp.length == 2 ? Integer.parseInt(brokenUp[1]) : null);
            }
        }
        return value;
    }
}
