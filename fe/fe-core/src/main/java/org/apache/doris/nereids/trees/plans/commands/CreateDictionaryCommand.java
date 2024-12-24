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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.CreateDictionaryInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DictionaryColumnDefinition;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Command for creating a dictionary.
 */
public class CreateDictionaryCommand extends Command {
    private static final Logger LOG = LogManager.getLogger(CreateDictionaryCommand.class);

    private final CreateDictionaryInfo createDictionaryInfo;

    public CreateDictionaryCommand(boolean ifNotExists, String dbName, String dictName, String sourceCtlName,
            String sourceDbName, String sourceTableName, List<DictionaryColumnDefinition> columns,
            Map<String, String> properties) {
        super(PlanType.CREATE_DICTIONARY_COMMAND);
        this.createDictionaryInfo = new CreateDictionaryInfo(ifNotExists, dbName, dictName, sourceCtlName, sourceDbName,
                sourceTableName, columns, properties);
    }

    public CreateDictionaryCommand(CreateDictionaryInfo createDictionaryInfo) {
        super(PlanType.CREATE_DICTIONARY_COMMAND);
        this.createDictionaryInfo = Objects.requireNonNull(createDictionaryInfo, "CreateDictionaryInfo cannot be null");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDictionaryCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DDL;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) {
        try {
            // 1. Validate the dictionary info. names and existence.
            createDictionaryInfo.validate(ctx);

            // 2. Create dictionary and save it in manager
            Dictionary dictionary = Env.getCurrentEnv().getDictionaryManager().createDictionary(createDictionaryInfo);

            LOG.info("Created dictionary {} in {} from {}", createDictionaryInfo.getDictName(),
                    createDictionaryInfo.getDbName(), createDictionaryInfo.getSourceTableName());

            // 3. Schedule the initial data load for the dictionary
            Env.getCurrentEnv().getDictionaryManager().scheduleDataLoad(dictionary);
        } catch (Exception e) {
            LOG.warn("Failed to create dictionary: {}", e.getMessage());
            throw new AnalysisException("Failed to create dictionary: " + e.getMessage());
        }
    }

    public CreateDictionaryInfo getCreateDictionaryInfo() {
        return createDictionaryInfo;
    }
}
