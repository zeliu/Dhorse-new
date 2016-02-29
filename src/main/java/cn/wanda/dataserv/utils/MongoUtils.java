package cn.wanda.dataserv.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public final class MongoUtils {

    private final static Map<String, String> default_params = new HashMap<String, String>();

    static {
        default_params.put("autoconnectretry", "true");
        default_params.put("connecttimeoutms", "18000000");
        default_params.put("sockettimeoutms", "18000000");
    }

    /**
     * @param uri : mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database[.collection]][?options]]
     * @return
     */
    public static DBCollection getCollection(String uri, String database, String collection) {

        uri = processURI(uri);

        MongoClientURI mUri = getMongoURI(uri);

        if (StringUtils.isBlank(collection)) {
            collection = mUri.getCollection();
        }

        if (StringUtils.isBlank(database)) {
            database = mUri.getDatabase();
        }

        return getCollection(mUri, database, collection);
    }

    public static DBCollection getCollection(String uri, String collection) {

        uri = processURI(uri);

        MongoClientURI mUri = getMongoURI(uri);

        if (StringUtils.isBlank(collection)) {
            collection = mUri.getCollection();
        }

        return getCollection(mUri, mUri.getDatabase(), collection);
    }

    public static DBCollection getCollection(String uri) {

        uri = processURI(uri);

        MongoClientURI mUri = getMongoURI(uri);

        return getCollection(mUri, mUri.getDatabase(), mUri.getCollection());
    }

    private static DBCollection getCollection(MongoClientURI mUri,
                                              String database, String collection) {

        DBCollection coll;
        try {
            MongoClient mongo = new MongoClient(mUri);
            DB db = mongo.getDB(database);
            if (!db.isAuthenticated() && mUri.getUsername() != null
                    && mUri.getPassword() != null) {
                db.authenticate(mUri.getUsername(), mUri.getPassword());
            }

            if (!db.collectionExists(collection)) {
                db.createCollection(collection, null);
            }
            coll = db.getCollection(collection);

            return coll;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Couldn't connect and authenticate to get collection", e);
        }
    }

    public static MongoClientURI getMongoURI(String uri) {
        return new MongoClientURI(uri);
    }

    public static String processURI(String uri) {

        for (String key : default_params.keySet()) {
            if (!uri.contains(key)) {
                if (!uri.contains("?")) {
                    uri = uri + "?" + key + "=" + default_params.get(key);
                } else {
                    uri = uri + "&" + key + "=" + default_params.get(key);
                }
            }
        }
        return uri;
    }
}