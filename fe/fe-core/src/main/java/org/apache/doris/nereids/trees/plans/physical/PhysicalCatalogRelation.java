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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.runtimefilterv2.RuntimeFilterV2;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * relation generated from TableIf
 */
public abstract class PhysicalCatalogRelation extends PhysicalRelation implements CatalogRelation {

    protected final TableIf table;
    protected final ImmutableList<String> qualifier;
    protected final ImmutableList<Slot> operativeSlots;

    /**
     * Constructor for PhysicalCatalogRelation.
     *
     * @param table Doris table
     * @param qualifier qualified relation name
     */
    public PhysicalCatalogRelation(RelationId relationId, PlanType type, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            Collection<Slot> operativeSlots) {
        super(relationId, type, groupExpression, logicalProperties);
        this.table = Objects.requireNonNull(table, "table can not be null");
        this.qualifier = ImmutableList.copyOf(Objects.requireNonNull(qualifier, "qualifier can not be null"));
        this.operativeSlots = ImmutableList.copyOf(Objects.requireNonNull(operativeSlots,
                "operativeSlots can not be null"));
    }

    /**
     * Constructor for PhysicalCatalogRelation.
     *
     * @param table Doris table
     * @param qualifier qualified relation name
     */
    public PhysicalCatalogRelation(RelationId relationId, PlanType type, TableIf table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties,
            Statistics statistics,
            Collection<Slot> operativeSlots) {
        super(relationId, type, groupExpression, logicalProperties, physicalProperties, statistics);
        this.table = Objects.requireNonNull(table, "table can not be null");
        this.qualifier = ImmutableList.copyOf(Objects.requireNonNull(qualifier, "qualifier can not be null"));
        this.operativeSlots = ImmutableList.copyOf(Objects.requireNonNull(operativeSlots,
                "operativeSlots can not be null"));
    }

    @Override
    public TableIf getTable() {
        return table;
    }

    @Override
    public DatabaseIf getDatabase() throws AnalysisException {
        Preconditions.checkArgument(!qualifier.isEmpty(), "qualifier can not be empty");
        try {
            int len = qualifier.size();
            if (2 == len) {
                CatalogIf<DatabaseIf> catalog = Env.getCurrentEnv().getCatalogMgr()
                        .getCatalogOrAnalysisException(qualifier.get(0));
                return catalog.getDbOrAnalysisException(qualifier.get(1));
            } else if (1 == len) {
                CatalogIf<DatabaseIf> catalog = Env.getCurrentEnv().getCurrentCatalog();
                return catalog.getDbOrAnalysisException(qualifier.get(0));
            } else if (0 == len) {
                CatalogIf<DatabaseIf> catalog = Env.getCurrentEnv().getCurrentCatalog();
                ConnectContext ctx = ConnectContext.get();
                return catalog.getDb(ctx.getDatabase()).get();
            }
            return null;
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    @Override
    public List<Slot> computeOutput() {
        return table.getBaseSchema()
                .stream()
                .map(col -> SlotReference.fromColumn(table, col, qualified()))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public List<String> getQualifier() {
        return qualifier;
    }

    /**
     * Full qualified name parts, i.e., concat qualifier and name into a list.
     */
    public List<String> qualified() {
        return Utils.qualifiedNameParts(qualifier, table.getName());
    }

    /**
     * Full qualified table name, concat qualifier and name with `.` as separator.
     */
    public String qualifiedName() {
        return Utils.qualifiedName(qualifier, table.getName());
    }

    @Override
    public boolean canPushDownRuntimeFilter() {
        return true;
    }

    @Override
    public String shapeInfo() {
        StringBuilder shapeBuilder = new StringBuilder();
        shapeBuilder.append(this.getClass().getSimpleName())
                .append("[").append(table.getName()).append("]");
        if (!getAppliedRuntimeFilters().isEmpty()) {
            shapeBuilder.append(" apply RFs:");
            getAppliedRuntimeFilters()
                    .stream().forEach(rf -> shapeBuilder.append(" RF").append(rf.getId().asInt()));
        }
        if (!runtimeFiltersV2.isEmpty()) {
            shapeBuilder.append(" RFV2:");
            for (RuntimeFilterV2 rfv2 : runtimeFiltersV2) {
                shapeBuilder.append(" RF").append(rfv2.getId().asInt());
            }
        }
        return shapeBuilder.toString();
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        Set<Slot> outputSet = Utils.fastToImmutableSet(getOutputSet());
        for (PrimaryKeyConstraint c : table.getPrimaryKeyConstraints()) {
            Set<Column> columns = c.getPrimaryKeys(table);
            builder.addUniqueSlot((ImmutableSet) findSlotsByColumn(outputSet, columns));
        }

        for (UniqueConstraint c : table.getUniqueConstraints()) {
            Set<Column> columns = c.getUniqueKeys(table);
            builder.addUniqueSlot((ImmutableSet) findSlotsByColumn(outputSet, columns));
        }
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        // No uniform slot for catalog relation
    }

    private ImmutableSet<SlotReference> findSlotsByColumn(Set<Slot> outputSet, Set<Column> columns) {
        ImmutableSet.Builder<SlotReference> slotSet = ImmutableSet.builderWithExpectedSize(columns.size());
        for (Slot slot : outputSet) {
            if (!(slot instanceof SlotReference)) {
                continue;
            }
            SlotReference slotRef = (SlotReference) slot;
            if (slotRef.getOriginalColumn().isPresent() && columns.contains(slotRef.getOriginalColumn().get())) {
                slotSet.add(slotRef);
            }
        }
        return slotSet.build();
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        // don't generate any equal pair
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        // don't generate any equal pair
    }
}
