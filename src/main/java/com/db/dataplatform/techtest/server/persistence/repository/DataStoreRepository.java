package com.db.dataplatform.techtest.server.persistence.repository;

import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DataStoreRepository extends JpaRepository<DataBodyEntity, Long> {

    @Query("SELECT db FROM DataBodyEntity db WHERE db.dataHeaderEntity.blocktype = :blocktype")
    List<DataBodyEntity> findByBlockTypeEnum(@Param("blocktype") BlockTypeEnum blocktype);

    @Query("SELECT db FROM DataBodyEntity db WHERE db.dataHeaderEntity.name = :name")
    Optional<DataBodyEntity> findByBlockName(@Param("name") String name);

}
