package cn.wanda.dataserv.output.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import cn.wanda.dataserv.config.FieldConfig;
import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.SchemaConfig;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.location.MongoOutputLocation;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.output.AbstractOutput;
import cn.wanda.dataserv.output.OutputException;
import cn.wanda.dataserv.utils.MongoUtils;
import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;


@Log4j
public class MongoDBOutput extends AbstractOutput {

    private DBCollection dbCollection;

    private String[] cols;

    /**
     * type: <br>
     * 1: string
     * 2: int
     * 3: long
     * 4: double
     * default: string
     */
    private int[] colsType;

    private String fieldsSeparator = "\t";

    private Pattern p;

    private List<DBObject> cache;

    private int cacheSize = 0;

    private int cacheCapacity = 10000;

    @Override
    public void writeLine(Line line) {
        if (cacheSize < cacheCapacity) {
            cache.add(getDBObject(line));
            cacheSize++;
        } else {
            dbCollection.insert(cache);
            cache = new ArrayList<DBObject>();
            cacheSize = 0;
        }
    }

    private DBObject getDBObject(Line line) {
        BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
        String s = line.getLine();
//		String[] values = p.split(s, 0);
        String[] values = StringUtils.splitByWholeSeparatorPreserveAllTokens(s, this.fieldsSeparator);
        int min = cols.length > values.length ? values.length : cols.length;
        for (i = 0; i < min; i++) {
            Object value = null;
            switch (colsType[i]) {
                case 1:
                    value = values[i];
                    break;
                case 2:
                    value = Integer.parseInt(values[i]);
                    break;
                case 3:
                    value = Long.parseLong(values[i]);
                    break;
                case 4:
                    value = Double.parseDouble(values[i]);
                    break;
                default:
                    value = values[i];
            }
            builder.append(cols[i], value);
        }
        return builder.get();
    }

    private int i = 0;

    @Override
    public void close() {
        try {
            if (cacheSize > 0) {
                dbCollection.insert(cache);
            }
            if (dbCollection != null) {
                DB db = dbCollection.getDB();
                if (db != null) {
                    db.cleanCursors(true);
                    Mongo mongo = db.getMongo();
                    if (mongo != null) {
                        mongo.close();
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Close Mongo output failed!", e);
        }

    }

    @Override
    public void init() {
        //process params
        LocationConfig l = this.outputConfig.getLocation();

        if (!MongoOutputLocation.class.isAssignableFrom(l.getClass())) {
            throw new OutputException(
                    "output config type is not MongoOutputLocation!");
        }

        // init cols
        SchemaConfig schema = this.outputConfig.getSchema();
        if (schema == null) {
            throw new OutputException(
                    "output config: schema is null!");
        }

        List<FieldConfig> fileds = schema.getFields();
        if (fileds == null || fileds.size() == 0) {
            throw new OutputException(
                    "output config: fileds is null!");
        }

        this.cols = new String[fileds.size()];
        this.colsType = new int[fileds.size()];

        for (int i = 0; i < fileds.size(); i++) {
            this.cols[i] = fileds.get(i).getName();
            String type = fileds.get(i).getType();
            if ("int".equalsIgnoreCase(type)) {
                this.colsType[i] = 2;
            } else if ("long".equalsIgnoreCase(type) || "bigint".equalsIgnoreCase(type)) {
                this.colsType[i] = 3;
            } else if ("double".equalsIgnoreCase(type) || "decimal".equalsIgnoreCase(type)) {
                this.colsType[i] = 4;
            } else {
                this.colsType[i] = 1;
            }
        }
        // init cols finished

        this.fieldsSeparator = this.outputConfig.getSchema().getFieldDelim();

        this.p = Pattern.compile(this.fieldsSeparator);

        MongoOutputLocation location = (MongoOutputLocation) l;

        String uri = location.getMongo().getUri();

        String database = ExpressionUtils.getOneValue(location.getDatabase());

        String collection = ExpressionUtils.getOneValue(location.getCollection());

        this.dbCollection = MongoUtils.getCollection(uri, database, collection);

        if ("false".equalsIgnoreCase(location.getIsAppend())) {
            dbCollection.drop();
        }

        this.cacheCapacity = this.runtimeConfig.getLineLimit();

        this.cache = new ArrayList<DBObject>(cacheCapacity);
    }
}