package org.apache.ignite.sqltests.models;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.sqltests.Issue24hf;

/**
 * Subscription model
 */
public class Subscription {

    public static final String CACHE = "subscription";
    public static final int SUBSCRIPTION_STATUS_CARDINALITY = 10;
    public static final int EXTERNAL_STATUS_CARDINALITY = 5;
    public static final int VERSION_CARDINALITY = 10;

    // comment for KeyPolicy.AFFINITY
    @QuerySqlField(index = true, inlineSize = 45)
    private String id;

    // comment for KeyPolicy.AFFINITY
    @QuerySqlField(index = true, inlineSize = 45)
    private String accountId;

    @QuerySqlField
    private long version;

    @QuerySqlField(index = true, inlineSize = 45)
    private String name;

    @QuerySqlField
    private long subscriptionStatus;

    @QuerySqlField
    private long externalStatus;

    // comment for KeyPolicy.AFFINITY
    public String getId() {
        return id;
    }

    public void setId(String id) {
        // comment for KeyPolicy.AFFINITY
        this.id = id;
    }

    // comment for KeyPolicy.AFFINITY
    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        // comment for KeyPolicy.AFFINITY
        this.accountId = accountId;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getSubscriptionStatus() {
        return subscriptionStatus;
    }

    public void setSubscriptionStatus(long subscriptionStatus) {
        this.subscriptionStatus = subscriptionStatus;
    }

    public long getExternalStatus() {
        return externalStatus;
    }

    public void setExternalStatus(long externalStatus) {
        this.externalStatus = externalStatus;
    }

    public static QueryEntity queryEntity() {
        switch (Issue24hf.KEY_POLICY) {
            case AFFINITY:
                return queryEntityAffinityKey();
            default:
                return queryEntitySubscriptionKey();
        }
    }

    public static QueryEntity queryEntityAffinityKey() {
        return new QueryEntity(AffinityKey.class, Subscription.class);
    }

    public static QueryEntity queryEntitySubscriptionKey() {
        return new QueryEntity(SubscriptionKey.class, Subscription.class);
    }

    public static class SubscriptionKey {

        @QuerySqlField(index = true, inlineSize = 45)
        private String id;

        @AffinityKeyMapped
        @QuerySqlField(index = true, inlineSize = 45)
        private String accountId;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getAccountId() {
            return accountId;
        }

        public void setAccountId(String accountId) {
            this.accountId = accountId;
        }

        @SuppressWarnings({"rawtypes", "unchecked", "SwitchStatementWithTooFewBranches"})
        public void putToCache(IgniteCache cache, Subscription val) {
            val.setId(id);
            val.setAccountId(accountId);
            switch (Issue24hf.KEY_POLICY) {
                case AFFINITY:
                    putToCacheAsAffinity(cache, val);
                    break;
                default:
                    putToCacheAsWrapper(cache, val);
            }
        }

        public void putToCacheAsAffinity(IgniteCache<AffinityKey<String>, Subscription> cache, Subscription val) {
            cache.put(new AffinityKey<>(id, accountId), val);
        }

        public void putToCacheAsWrapper(IgniteCache<SubscriptionKey, Subscription> cache, Subscription val) {
            cache.put(this, val);
        }

    }

}
