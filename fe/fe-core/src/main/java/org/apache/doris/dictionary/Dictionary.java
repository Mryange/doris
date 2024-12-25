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

package org.apache.doris.dictionary;

import org.apache.doris.catalog.Table;
import org.apache.doris.common.io.Text;
import org.apache.doris.nereids.trees.plans.commands.info.CreateDictionaryInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DictionaryColumnDefinition;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Dictionary metadata, including its structure and data source information. saved in
 */
public class Dictionary extends Table {

    @SerializedName(value = "dbName")
    private final String dbName;

    // dict name use base class's name

    @SerializedName(value = "sourceCtlName")
    private final String sourceCtlName;

    @SerializedName(value = "sourceDbName")
    private final String sourceDbName;

    @SerializedName(value = "sourceTableName")
    private final String sourceTableName;

    @SerializedName(value = "columns")
    private final List<DictionaryColumnDefinition> columns;

    @SerializedName(value = "properties")
    private final Map<String, String> properties;

    // createTime saved in base class

    @SerializedName(value = "lastUpdateTime")
    private long lastUpdateTime;

    public enum DictionaryStatus {
        LOADING, NORMAL, OUT_OF_DATE, REMOVING;
    }

    @SerializedName(value = "status")
    private DictionaryStatus status;

    public Dictionary(CreateDictionaryInfo info, long uniqueId) {
        super(uniqueId, info.getDictName(), TableType.DICTIONARY, null);
        this.dbName = info.getDbName();
        this.sourceCtlName = info.getSourceCtlName();
        this.sourceDbName = info.getSourceDbName();
        this.sourceTableName = info.getSourceTableName();
        this.columns = info.getColumns();
        this.properties = info.getProperties();
        this.lastUpdateTime = createTime;
        this.status = DictionaryStatus.NORMAL;
    }

    public String getDbName() {
        return dbName;
    }

    public String getSourceCtlName() {
        return sourceCtlName;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public List<DictionaryColumnDefinition> getDicColumns() {
        return columns;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public DictionaryStatus getStatus() {
        return status;
    }

    public void setStatus(DictionaryStatus status) {
        this.status = status;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Dictionary read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Dictionary.class);
    }

    @Override
    public String toString() {
        return "Dictionary{" + "dbName='" + dbName + '\'' + ", dicName='" + getName() + '\'' + ", sourceCtlName='"
                + sourceCtlName + '\'' + ", sourceDbName='" + sourceDbName + '\'' + ", sourceTableName='"
                + sourceTableName + '\'' + ", columns=" + columns + ", properties=" + properties + ", lastUpdateTime="
                + lastUpdateTime + ", status=" + status + '}';
    }
}
