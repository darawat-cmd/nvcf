/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.apikeys.persistance.repositories;

import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.COLUMN_DELETES_AT;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.COLUMN_EXPIRES_AT;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.COLUMN_ISSUER_SERVICE_ID;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.COLUMN_KEY_DETAILS;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.COLUMN_KEY_ID;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.COLUMN_KEY_STATUS;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.COLUMN_NCA_ID;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.COLUMN_OWNER_ID;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.COLUMN_OWNER_STATUS;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.COLUMN_OWNER_STATUS_UPDATED_AT;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.COLUMN_OWNER_TYPE;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel.TABLE_NAME;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndServiceModel;
import com.nvidia.apikeys.vo.KeyOwnerStatus;
import com.nvidia.apikeys.vo.KeyOwnerType;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.data.cassandra.repository.Consistency;
import org.springframework.data.cassandra.repository.MapIdCassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface KeyByOwnerAndServiceRepository extends
        MapIdCassandraRepository<KeyByOwnerAndServiceModel> {

    List<KeyByOwnerAndServiceModel> findByOwnerTypeAndOwnerIdAndIssuerServiceId(
            KeyOwnerType ownerType, String ownerId, String issuerServiceId);

    Optional<KeyByOwnerAndServiceModel> findByOwnerTypeAndOwnerIdAndIssuerServiceIdAndKeyId(
            KeyOwnerType ownerType, String ownerId, String issuerServiceId, String keyId);

    Optional<KeyByOwnerAndServiceModel> findFirstByOwnerTypeAndOwnerId(
            KeyOwnerType type, String id);

    List<KeyByOwnerAndServiceModel> findByOwnerTypeAndOwnerId(
            KeyOwnerType ownerType, String ownerId);

    @Consistency(DefaultConsistencyLevel.LOCAL_QUORUM)
    @Query("SELECT " + COLUMN_OWNER_TYPE
            + ", " + COLUMN_OWNER_ID
            + ", " + COLUMN_ISSUER_SERVICE_ID
            + ", " + COLUMN_KEY_ID
            + ", " + COLUMN_NCA_ID
            + ", " + COLUMN_OWNER_STATUS
            + ", " + COLUMN_OWNER_STATUS_UPDATED_AT
            + ", " + COLUMN_EXPIRES_AT
            + ", " + COLUMN_DELETES_AT
            + ", " + COLUMN_KEY_STATUS
            + ", " + COLUMN_KEY_DETAILS
            + ", WRITETIME(" + COLUMN_NCA_ID + ") AS wt_" + COLUMN_NCA_ID
            + ", WRITETIME(" + COLUMN_EXPIRES_AT + ") AS wt_" + COLUMN_EXPIRES_AT
            + ", WRITETIME(" + COLUMN_DELETES_AT + ") AS wt_" + COLUMN_DELETES_AT
            + ", WRITETIME(" + COLUMN_KEY_STATUS + ") AS wt_" + COLUMN_KEY_STATUS
            + ", WRITETIME(" + COLUMN_KEY_DETAILS + ") AS wt_" + COLUMN_KEY_DETAILS
            + " FROM " + TABLE_NAME
            + " WHERE " + COLUMN_OWNER_TYPE + "=:ownerType"
            + " AND " + COLUMN_OWNER_ID + "=:ownerId"
            + " AND " + COLUMN_ISSUER_SERVICE_ID + "=:issuerServiceId"
            + " AND " + COLUMN_KEY_ID + "=:keyId")
    ResultSet findMigrationRow(@Param("ownerType") KeyOwnerType ownerType,
            @Param("ownerId") String ownerId, @Param("issuerServiceId") String issuerServiceId,
            @Param("keyId") String keyId);

    @Consistency(DefaultConsistencyLevel.LOCAL_QUORUM)
    @Query(value = "UPDATE " + TABLE_NAME
            + " USING TIMESTAMP :timestamp SET " + COLUMN_NCA_ID + "=:ncaId"
            + " WHERE " + COLUMN_OWNER_TYPE + "=:ownerType"
            + " AND " + COLUMN_OWNER_ID + "=:ownerId"
            + " AND " + COLUMN_ISSUER_SERVICE_ID + "=:issuerServiceId"
            + " AND " + COLUMN_KEY_ID + "=:keyId",
            idempotent = Query.Idempotency.IDEMPOTENT)
    ResultSet setNcaIdAtTimestamp(@Param("ownerType") KeyOwnerType ownerType,
            @Param("ownerId") String ownerId, @Param("issuerServiceId") String issuerServiceId,
            @Param("keyId") String keyId, @Param("ncaId") String ncaId,
            @Param("timestamp") long timestamp);

    @Query("INSERT INTO " + KeyByOwnerAndServiceModel.TABLE_NAME
            + "(" + COLUMN_OWNER_TYPE
            + "," + COLUMN_OWNER_ID
            + "," + COLUMN_OWNER_STATUS
            + "," + COLUMN_OWNER_STATUS_UPDATED_AT + ") "
            + " VALUES "
            + "(:" + COLUMN_OWNER_TYPE
            + ",:" + COLUMN_OWNER_ID
            + ",:" + COLUMN_OWNER_STATUS
            + ",:" + COLUMN_OWNER_STATUS_UPDATED_AT + ") ")
    ResultSet updateKeyOwner(
            @Param(COLUMN_OWNER_TYPE) KeyOwnerType ownerType,
            @Param(COLUMN_OWNER_ID) String ownerId,
            @Param(COLUMN_OWNER_STATUS) KeyOwnerStatus ownerStatus,
            @Param(COLUMN_OWNER_STATUS_UPDATED_AT) Instant ownerStatusUpdatedAt);

}
