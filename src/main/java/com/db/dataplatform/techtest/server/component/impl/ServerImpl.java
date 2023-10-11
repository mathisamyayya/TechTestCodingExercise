package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private final DataBodyService dataBodyServiceImpl;
    private final ModelMapper modelMapper;

    /**
     * @param envelope
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope) throws IOException, NoSuchAlgorithmException {
        //Exercise 2
        String checksum = calculateMD5Checksum(envelope.getDataBody().getDataBody());
        if (envelope.getHash().equals(checksum)) {
            log.info("Checksum matches with the body of the incoming block");
            // Save to persistence.
            persist(envelope);
            log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());
            return true;
        }
        return false;
    }

    private String calculateMD5Checksum(String body) throws NoSuchAlgorithmException {
        String checksumAlgorithm = "MD5";
        MessageDigest md5 = MessageDigest.getInstance(checksumAlgorithm);
        byte[] md5Checksum = md5.digest(body.getBytes());

        StringBuilder md5Hex = new StringBuilder();
        for (byte b : md5Checksum) {
            md5Hex.append(String.format("%02x", b));
        }
        return md5Hex.toString();
    }

    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);

        saveData(dataBodyEntity);
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }

    /**
     * @param blockType
     * @return List<DataEnvelope>  if there is a match with the blocktype.
     */
    @Override
    public List<DataEnvelope> getDataEnvelope(BlockTypeEnum blockType) {

        // retrieve by block type.
        List<DataEnvelope> envelopes = retrieve(blockType);

        log.info("Data retrieved successfully, data envelope size: {}", envelopes.size());
        return envelopes;
    }

    private List<DataEnvelope> retrieve(BlockTypeEnum blockType) {
        log.info("Retrieving data by blockType: {}", blockType);
        List<DataBodyEntity> dataBodyEntities = retrieveAllData(blockType);
        return dataBodyEntities.stream().map(dataBodyEntity -> {
            DataHeader dataHeader = new DataHeader(dataBodyEntity.getDataHeaderEntity().getName(), dataBodyEntity.getDataHeaderEntity().getBlocktype());
            DataBody dataBody = new DataBody(dataBodyEntity.getDataBody());
            return new DataEnvelope(Strings.EMPTY, dataHeader, dataBody);
        }).collect(Collectors.toList());
    }

    private List<DataBodyEntity> retrieveAllData(BlockTypeEnum blockType) {
        return dataBodyServiceImpl.getDataByBlockType(blockType);
    }

    @Override
    public boolean updateDataEnvelope(String name, BlockTypeEnum newBlockType) {
        // retrieve by block name
        Optional<DataBodyEntity> envelope = retrieveByName(name);
        if (envelope.isPresent()) {
            DataBodyEntity dataBodyEntity = envelope.get();
            dataBodyEntity.getDataHeaderEntity().setBlocktype(newBlockType);
            saveData(dataBodyEntity);
            log.info("Data retrieved by block name {} successfully and updated blocked type: {}", dataBodyEntity.getDataHeaderEntity().getName(), dataBodyEntity.getDataHeaderEntity().getBlocktype());
            return true;
        }
        return false;
    }

    private Optional<DataBodyEntity> retrieveByName(String blockName) {
        return dataBodyServiceImpl.getDataByBlockName(blockName);
    }

}
