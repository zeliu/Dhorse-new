package cn.wanda.dataserv.utils.elasticsearch;

import lombok.extern.log4j.Log4j;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.collect.Sets.newHashSet;

/**
 * Created by songzhuozhuo on 2015/4/1
 */
@Log4j
public abstract class BaseTransportClient {

    private final Set<InetSocketTransportAddress> addresses = newHashSet();

    protected TransportClient client;

    protected ConfigHelper configHelper = new ConfigHelper();

    private boolean isShutdown;

    protected void createClient(Settings settings) {
        if (client != null) {
            log.warn("client is open, closing...");
            client.close();
            client.threadPool().shutdown();
            log.warn("client is closed");
            client = null;
        }
        if (settings != null) {
            log.info(String.format("creating transport client, java version %s, effective settings %s",
                    System.getProperty("java.version"), settings.getAsMap()));
            // false = do not load config settings from environment
            this.client = new TransportClient(settings, false);
            log.info(String.format("transport client settings = %s", client.settings().getAsMap()));
        } else {
            log.info(String.format("creating transport client, java version %s, using default settings",
                    System.getProperty("java.version")));
            this.client = new TransportClient();
        }

        try {
            connect(settings);
        } catch (UnknownHostException e) {
            log.error(e.getMessage(), e);
        } catch (SocketException e) {
            log.error(e.getMessage(), e);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public Client client() {
        return client;
    }

    public List<String> getConnectedNodes() {
        return ClientHelper.getConnectedNodes(client);
    }

    public synchronized void shutdown() {
        if (client != null) {
            log.debug("shutdown started");
            client.close();
            client.threadPool().shutdown();
            client = null;
            log.debug("shutdown complete");
        }
        addresses.clear();
        isShutdown = true;
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    protected Settings findSettings() {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put("host", "localhost");
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            log.debug(String.format("the hostname is %s", hostname));
            settingsBuilder.put("host", hostname)
                    .put("port", 9300);
        } catch (UnknownHostException e) {
            log.warn("can't resolve host name, probably something wrong with network config: " + e.getMessage(), e);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
        return settingsBuilder.build();
    }

    protected void connect(Settings settings) throws IOException {
        String hostname = settings.get("host");
        int port = settings.getAsInt("port", 9300);
        switch (hostname) {
            case "hostname": {
                InetSocketTransportAddress address = new InetSocketTransportAddress(InetAddress.getLocalHost().getHostName(), port);
                if (!addresses.contains(address)) {
                    log.info(String.format("adding hostname address for transport client: %s", address));
                    client.addTransportAddress(address);
                    addresses.add(address);
                }
                break;
            }
            case "interfaces": {
                Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
                for (NetworkInterface netint : Collections.list(nets)) {
                    log.info(String.format("checking network interface = %s", netint.getName()));
                    Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                    for (InetAddress addr : Collections.list(inetAddresses)) {
                        log.info(String.format("checking address =  %s", netint.getName()));
                        InetSocketTransportAddress address = new InetSocketTransportAddress(addr, port);
                        if (!addresses.contains(address)) {
                            log.info(String.format("adding address to transport client: %s", address));
                            client.addTransportAddress(address);
                            addresses.add(address);
                        }
                    }
                }
                break;
            }
            case "inet4": {
                Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
                for (NetworkInterface netint : Collections.list(nets)) {
                    Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                    for (InetAddress addr : Collections.list(inetAddresses)) {
                        if (addr instanceof Inet4Address) {
                            InetSocketTransportAddress address = new InetSocketTransportAddress(addr, port);
                            if (!addresses.contains(address)) {
                                client.addTransportAddress(address);
                                addresses.add(address);
                            }
                        }
                    }
                }
                break;
            }
            case "inet6": {
                Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
                for (NetworkInterface netint : Collections.list(nets)) {
                    Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
                    for (InetAddress addr : Collections.list(inetAddresses)) {
                        if (addr instanceof Inet6Address) {
                            InetSocketTransportAddress address = new InetSocketTransportAddress(addr, port);
                            if (!addresses.contains(address)) {
                                client.addTransportAddress(address);
                                addresses.add(address);
                            }
                        }
                    }
                }
                break;
            }
            default: {
                InetSocketTransportAddress address = new InetSocketTransportAddress(hostname, port);
                if (!addresses.contains(address)) {
                    log.info(String.format("adding custom address for transport client: %s", address));
                    client.addTransportAddress(address);
                    addresses.add(address);
                }
                break;
            }
        }
        log.info(String.format("configured addresses to connect = %s ...", addresses));
        if (client.connectedNodes() != null) {
            List<DiscoveryNode> nodes = client.connectedNodes().asList();
            log.info(String.format("connected nodes = %s", nodes));
            for (DiscoveryNode node : nodes) {
                log.info(String.format("new connection to %s", nodes));
            }
            if (!nodes.isEmpty()) {
                if (settings.get("sniff") != null || settings.get("es.sniff") != null || settings.get("client.transport.sniff") != null) {
                    try {
//                        connectMore();
                    } catch (Exception e) {
                        log.error("error while connecting to more nodes", e);
                    }
                }
            }
        }
    }

    public ImmutableSettings.Builder getSettingsBuilder() {
        return configHelper.settingsBuilder();
    }

    public void resetSettings() {
        configHelper.reset();
    }

    public void setting(InputStream in) throws IOException {
        configHelper.setting(in);
    }

    public void addSetting(String key, String value) {
        configHelper.setting(key, value);
    }

    public void addSetting(String key, Boolean value) {
        configHelper.setting(key, value);
    }

    public void addSetting(String key, Integer value) {
        configHelper.setting(key, value);
    }

    public void setSettings(Settings settings) {
        configHelper.settings(settings);
    }

    public Settings getSettings() {
        return configHelper.settings();
    }

    public void mapping(String type, String mapping) throws IOException {
        configHelper.mapping(type, mapping);
    }

    public void mapping(String type, InputStream in) throws IOException {
        configHelper.mapping(type, in);
    }

    public Map<String, String> getMappings() {
        return configHelper.mappings();
    }

    private void connectMore() throws IOException {
        log.debug("trying to discover more nodes...");
        ClusterStateResponse clusterStateResponse = client.admin().cluster().state(new ClusterStateRequest()).actionGet();
        DiscoveryNodes nodes = clusterStateResponse.getState().getNodes();
        for (DiscoveryNode node : nodes) {
            log.debug(String.format("adding discovered node %s", node));
            try {
                client.addTransportAddress(node.address());
            } catch (Exception e) {
                log.warn("can't add node " + node, e);
            }
        }
        log.debug("... discovery done");
    }

}
