package com.db.dataplatform.techtest.server.component;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public interface Server {
    boolean saveDataEnvelope(DataEnvelope envelope) throws IOException, NoSuchAlgorithmException;

    List<DataEnvelope> getDataEnvelope(BlockTypeEnum blockType);

    boolean updateDataEnvelope(String name, BlockTypeEnum newBlockType);

    void callHadoopDataLakeService(RestTemplate restTemplate, String url, DataEnvelope dataEnvelope) throws HadoopClientException;

    String recoverFromFailure(HadoopClientException hex, RestTemplate restTemplate, String url, DataEnvelope dataEnvelope);
}
