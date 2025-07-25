// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.paimon;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PaimonTableCache {
    // Max cache num of paimon table
    public static final long max_external_schema_cache_num = 50;
    // The expiration time of a cache object after last access of it.
    public static final long external_cache_expire_time_minutes_after_access = 100;

    private static final Logger LOG = LoggerFactory.getLogger(PaimonTableCache.class);

    private static LoadingCache<PaimonTableCacheKey, TableExt> tableCache = CacheBuilder.newBuilder()
            .maximumSize(max_external_schema_cache_num)
            .expireAfterAccess(external_cache_expire_time_minutes_after_access, TimeUnit.MINUTES)
            .build(new CacheLoader<PaimonTableCacheKey, TableExt>() {
                @Override
                public TableExt load(PaimonTableCacheKey key) {
                    return loadTable(key);
                }
            });

    private static TableExt loadTable(PaimonTableCacheKey key) {
        try {
            LOG.warn("load table:{}", key);
            Catalog catalog = createCatalog(key.getPaimonOptionParams(), key.getHadoopOptionParams());
            Table table;
            if (key.getQueryType() != null) {
                table = catalog.getTable(new Identifier(key.getDbName(), key.getTblName(),
                        null, key.getQueryType()));
            } else {
                table = catalog.getTable(Identifier.create(key.getDbName(), key.getTblName()));
            }
            return new TableExt(table, System.currentTimeMillis());
        } catch (Catalog.TableNotExistException e) {
            LOG.warn("failed to create paimon table ", e);
            throw new RuntimeException(e);
        }
    }

    private static Catalog createCatalog(
            Map<String, String> paimonOptionParams,
            Map<String, String> hadoopOptionParams) {
        Options options = new Options();
        paimonOptionParams.entrySet().stream().forEach(kv -> options.set(kv.getKey(), kv.getValue()));
        Configuration hadoopConf = new Configuration();
        hadoopOptionParams.entrySet().stream().forEach(kv -> hadoopConf.set(kv.getKey(), kv.getValue()));
        CatalogContext context = CatalogContext.create(options, hadoopConf);
        return CatalogFactory.createCatalog(context);
    }

    public static void invalidateTableCache(PaimonTableCacheKey key) {
        tableCache.invalidate(key);
    }

    public static TableExt getTable(PaimonTableCacheKey key) {
        try {
            return tableCache.get(key);
        } catch (ExecutionException e) {
            throw new RuntimeException("failed to get table for:" + key);
        }
    }

    public static class TableExt {
        private Table table;
        private long createTime;

        public TableExt(Table table, long createTime) {
            this.table = table;
            this.createTime = createTime;
        }

        public Table getTable() {
            return table;
        }

        public long getCreateTime() {
            return createTime;
        }
    }

    public static class PaimonTableCacheKey {
        // in key
        private final long ctlId;
        private final long dbId;
        private final long tblId;

        // not in key
        private Map<String, String> paimonOptionParams;
        private Map<String, String> hadoopOptionParams;
        private String dbName;
        private String tblName;
        private String queryType;

        public PaimonTableCacheKey(long ctlId, long dbId, long tblId,
                Map<String, String> paimonOptionParams,
                Map<String, String> hadoopOptionParams,
                String dbName, String tblName) {
            this.ctlId = ctlId;
            this.dbId = dbId;
            this.tblId = tblId;
            this.paimonOptionParams = paimonOptionParams;
            this.hadoopOptionParams = hadoopOptionParams;
            this.dbName = dbName;
            this.tblName = tblName;
        }

        public PaimonTableCacheKey(long ctlId, long dbId, long tblId,
                Map<String, String> paimonOptionParams,
                Map<String, String> hadoopOptionParams,
                String dbName, String tblName, String queryType) {
            this.ctlId = ctlId;
            this.dbId = dbId;
            this.tblId = tblId;
            this.paimonOptionParams = paimonOptionParams;
            this.hadoopOptionParams = hadoopOptionParams;
            this.dbName = dbName;
            this.tblName = tblName;
            this.queryType = queryType;
        }

        public long getCtlId() {
            return ctlId;
        }

        public long getDbId() {
            return dbId;
        }

        public long getTblId() {
            return tblId;
        }

        public Map<String, String> getPaimonOptionParams() {
            return paimonOptionParams;
        }

        public Map<String, String> getHadoopOptionParams() {
            return hadoopOptionParams;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTblName() {
            return tblName;
        }

        public String getQueryType() {
            return queryType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PaimonTableCacheKey that = (PaimonTableCacheKey) o;
            return ctlId == that.ctlId && dbId == that.dbId && tblId == that.tblId && Objects.equal(
                    queryType,
                    that.queryType);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(ctlId, dbId, tblId);
        }

        @Override
        public String toString() {
            return "PaimonTableCacheKey{"
                    + "ctlId=" + ctlId
                    + ", dbId=" + dbId
                    + ", tblId=" + tblId
                    + ", paimonOptionParams=" + paimonOptionParams
                    + ", hadoopOptionParams=" + hadoopOptionParams
                    + ", dbName='" + dbName + '\''
                    + ", tblName='" + tblName + '\''
                    + ", queryType='" + queryType + '\''
                    + '}';
        }
    }

}
