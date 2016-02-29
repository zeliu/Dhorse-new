package cn.wanda.dataserv.output.impl;

import static cn.wanda.dataserv.utils.ConvertUtils.get;
import static cn.wanda.dataserv.utils.ConvertUtils.getAsBoolean;
import static cn.wanda.dataserv.utils.ConvertUtils.getAsBytesSize;
import static cn.wanda.dataserv.utils.ConvertUtils.getAsInt;
import static cn.wanda.dataserv.utils.ConvertUtils.getAsTime;
import static cn.wanda.dataserv.utils.ConvertUtils.readFromJsonStr;

import java.io.IOException;
import java.util.Map;

import cn.wanda.dataserv.config.resource.ESServer;
import lombok.extern.log4j.Log4j;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.location.ESOutputLocation;
import cn.wanda.dataserv.config.resource.ESContext;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.output.AbstractOutput;
import cn.wanda.dataserv.output.OutputException;
import cn.wanda.dataserv.utils.elasticsearch.Ingest;
import cn.wanda.dataserv.utils.elasticsearch.IngestFactory;
import cn.wanda.dataserv.utils.elasticsearch.Metric;
import cn.wanda.dataserv.utils.elasticsearch.transport.BulkTransportClient;

/**
 * Created by songzhuozhuo on 2015/4/3
 */
@Log4j
public class ESOutput extends AbstractOutput {

    ESContext esContext;
    ESServer esServer;

    ESOutputLocation location;

    protected IngestFactory ingestFactory;

    protected Ingest ingest;

    protected Metric metric;

    protected Settings indexSettings;

    protected Map<String, String> indexMappings;

    protected String index;

    protected String type;
    
    protected String id;
    
    protected String indexIdDelim;

    protected boolean indexCovered;

    protected String parent;
    
    protected volatile boolean suspended = false;

    @Override
    public void writeLine(Line line) {
        String jsonstr = line.getLine();
        try {
            Map<String, Object> value = readFromJsonStr(jsonstr, Map.class);
            IndexRequest request = Requests.indexRequest(this.index)
                    .type(this.type)
                    .source(value);
            if(id!=null)
            	request.id(value.get(id).toString());
            if(parent!=null)
            	request.parent(value.get(parent).toString());
            if (log.isTraceEnabled()) {
                log.trace("adding bulk index action " + request.source().toUtf8());
            }

            if (ingest != null) {
                ingest.bulkIndex(request);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (ingest == null || ingest.isShutdown()) {
            ingest = ingestFactory.create();
        }
        try {
            flush();
            ingest.stopBulk(index);
            ingest.refresh(index);
            if (metric.indices() != null && !metric.indices().isEmpty()) {
                for (String index : ImmutableSet.copyOf(metric.indices())) {
                    log.info("stopping bulk mode for index {} and refreshing..." + index);
                    ingest.stopBulk(index);
                    ingest.refresh(index);
                }
            }
            if (!ingest.isShutdown()) {
                ingest.shutdown();
            }
        } catch (IOException e) {
            log.error(e);
            throw new OutputException("EsOutput close failed!");
        }

    }

    @Override
    public void init() {
        // process params
        LocationConfig l = this.outputConfig.getLocation();

        if (!ESOutputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("output config type is not mysql!");
        }

        location = (ESOutputLocation) l;

        esContext = location.getEsContext();
        esServer = location.getEsServer();

        this.ingestFactory = createIngestFactory(esContext);
        this.ingest = ingestFactory.create();
        
        this.index = esContext.getIndex();
        this.type = esContext.getType();
        this.metric = ingest.getMetric();
        this.indexCovered = esContext.getIndexCovered().trim().equals("true")?true:false;
       	this.id = esContext.getIndexId();
       	this.indexIdDelim = esContext.getIndexIdDelim();
       	this.parent = esContext.getParent();
        beforeWrite();
    }


    public void beforeWrite() {
        if (ingest == null || ingest.isShutdown()) {
            ingest = ingestFactory.create();
        }
        try {
        	IndicesExistsResponse response = ingest.client().admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        	if(!response.isExists()){
        		ingest.newIndex(index);
        	}else{
        		if(indexCovered){
            		ingest.deleteIndex(index);
            		ingest.newIndex(index);
            	}
        	}
            long startRefreshInterval = -1L;
            long stopRefreshInterval = 1000L;

            ingest.startBulk(index, startRefreshInterval, stopRefreshInterval);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void prepare() {

    }

    private IngestFactory createIngestFactory(final ESContext esContext) {
        return new IngestFactory() {
            @Override
            public Ingest create() {
                Integer maxbulkactions = getAsInt(esContext.getMaxBulkActions(), 10000);
                Integer maxconcurrentbulkrequests = getAsInt(esContext.getMaxConcurrentBulkRequests(),
                        Runtime.getRuntime().availableProcessors() * 2);
                ByteSizeValue maxvolume = getAsBytesSize(esContext.getMaxBulkVolume(), ByteSizeValue.parseBytesSizeValue("10m"));
                TimeValue maxrequestwait = getAsTime(esContext.getMaxRequestWait(), TimeValue.timeValueSeconds(60));
                TimeValue flushinterval = getAsTime(esContext.getFlushInterval(), TimeValue.timeValueSeconds(5));
                BulkTransportClient ingest = new BulkTransportClient();
                Settings clientSettings = ImmutableSettings.settingsBuilder()
                        .put("cluster.name", get(esServer.getCluster(), "elasticsearch"))
                        .put("host", get(esServer.getHost(), "localhost"))
                        .put("port", getAsInt(esServer.getPort(), 9300))
                        .put("sniff", getAsBoolean(esContext.getSniff(), false))
                        .put("name", "dhorse") //  marks this node as "dhorse"
                        .put("client.transport.ignore_cluster_name", true) // ignore cluster name setting
                        .put("client.transport.ping_timeout", getAsTime(esContext.getTimeout(), TimeValue.timeValueSeconds(10))) //  ping timeout
                        .put("client.transport.nodes_sampler_interval", getAsTime(esContext.getTimeout(), TimeValue.timeValueSeconds(5))) // for sniff sampling
                        .build();
                ingest.maxActionsPerBulkRequest(maxbulkactions)
                        .maxConcurrentBulkRequests(maxconcurrentbulkrequests)
                        .maxVolumePerBulkRequest(maxvolume)
                        .maxRequestWait(maxrequestwait)
                        .flushIngestInterval(flushinterval)
                        .newClient(clientSettings);
                return ingest;
            }
        };
    }

    public void flush() throws IOException {
        if (ingest != null) {
            ingest.flushIngest();
            // wait for all outstanding bulk requests before continue with river
            try {
                ingest.waitForResponses(TimeValue.timeValueSeconds(60));
            } catch (InterruptedException e) {
                log.warn("interrupted while waiting for responses");
                Thread.currentThread().interrupt();
            }
        }
    }

    public void suspend() {
        if (ingest != null) {
            this.suspended = true;
            ingest.suspend();
        }
    }

    public void resume() {
        if (ingest != null) {
            this.suspended = false;
            ingest.resume();
        }
    }

    public synchronized void release() {
        try {
            flush();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public synchronized void shutdown() {
        try {
            flush();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        if (ingest != null && !ingest.isShutdown()) {
            // shut down ingest and release ingest resources
            ingest.shutdown();
        }
    }
}
