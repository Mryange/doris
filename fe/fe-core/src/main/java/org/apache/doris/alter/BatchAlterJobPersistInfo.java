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

package org.apache.doris.alter;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * used for batch log AlterJob to editLog in one atomic operation
 */
public class BatchAlterJobPersistInfo implements Writable {

    @SerializedName("l")
    private List<AlterJobV2> alterJobV2List;

    public BatchAlterJobPersistInfo(List<AlterJobV2> alterJobV2List) {
        this.alterJobV2List = alterJobV2List;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static BatchAlterJobPersistInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), BatchAlterJobPersistInfo.class);
    }

    public List<AlterJobV2> getAlterJobV2List() {
        return alterJobV2List;
    }
}
