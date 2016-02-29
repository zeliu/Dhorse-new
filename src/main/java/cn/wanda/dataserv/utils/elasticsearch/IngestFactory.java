package cn.wanda.dataserv.utils.elasticsearch;

/**
 * A factory for creating ingest objects
 */
public interface IngestFactory {

    Ingest create();
}
