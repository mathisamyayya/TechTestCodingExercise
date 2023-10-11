package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.component.Client;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import java.util.List;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ClientImpl implements Client {

    private final RestTemplate restTemplate;

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

    // Exercise 1
    @Override
    public void pushData(DataEnvelope dataEnvelope) {
        log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);

        HttpEntity<DataEnvelope> requestEntity = new HttpEntity<>(dataEnvelope);
        ResponseEntity<Boolean> responseEntity = restTemplate.exchange(
                URI_PUSHDATA,
                HttpMethod.POST,
                requestEntity,
                Boolean.class
        );
        log.info("Data pushed successfully after checksum validation is {}", responseEntity.getBody());
    }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        log.info("Query for data with header block type {}", blockType);
        String urlPath = URI_GETDATA.expand(blockType).toString();
        ParameterizedTypeReference<List<DataEnvelope>> responseType = new ParameterizedTypeReference<List<DataEnvelope>>() {
        };
        ResponseEntity<List<DataEnvelope>> responseEntity = restTemplate.exchange(urlPath, HttpMethod.GET, null, responseType);
        return responseEntity.getBody();
    }

    @Override
    public boolean updateData(String blockName, String newBlockType) {
        log.info("Updating blocktype to {} for block with name {}", newBlockType, blockName);
        String urlPath = URI_PATCHDATA.expand(blockName, newBlockType).toString();
        ResponseEntity<Boolean> responseEntity = restTemplate.exchange(urlPath, HttpMethod.PUT, null, Boolean.class);
        return Boolean.TRUE.equals(responseEntity.getBody());
    }


}
