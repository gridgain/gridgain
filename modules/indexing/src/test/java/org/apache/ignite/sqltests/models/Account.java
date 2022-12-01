package org.apache.ignite.sqltests.models;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.sqltests.Issue24hf;

/**
 * Account model
 */
public class Account {

    public static final String CACHE = "account";

    // comment for KeyPolicy.AFFINITY
    @QuerySqlField(index = true, inlineSize = 45)
    private String id;

    @QuerySqlField(index = true, inlineSize = 45)
    private String name;

    @QuerySqlField(index = true, inlineSize = 78)
    private long number;

    // comment for KeyPolicy.AFFINITY
    public String getId() {
        return id;
    }

    public void setId(String id) {
        // comment for KeyPolicy.AFFINITY
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getNumber() {
        return number;
    }

    public void setNumber(long number) {
        this.number = number;
    }

    public static QueryEntity queryEntity() {
        switch (Issue24hf.KEY_POLICY) {
            case AFFINITY:
                return queryEntityStringKey();
            default:
                return queryEntityAccountKey();
        }
    }

    public static QueryEntity queryEntityStringKey() {
        return new QueryEntity(String.class, Account.class);
    }

    public static QueryEntity queryEntityAccountKey() {
        return new QueryEntity(AccountKey.class, Account.class);
    }

    public static class AccountKey {

        @AffinityKeyMapped
        @QuerySqlField(index = true, inlineSize = 45)
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public void putToCache(IgniteCache cache, Account val) {
            val.setId(id);
            switch (Issue24hf.KEY_POLICY) {
                case AFFINITY:
                    putToCacheAsString(cache, val);
                    break;
                default:
                    putToCacheAsWrapper(cache, val);
            }
        }

        public void putToCacheAsString(IgniteCache<String, Account> cache, Account val) {
            cache.put(id, val);
        }

        public void putToCacheAsWrapper(IgniteCache<AccountKey, Account> cache, Account val) {
            cache.put(this, val);
        }

    }

}
