package com.db.dataplatform.techtest.server.api.controller;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static org.springframework.http.HttpStatus.GATEWAY_TIMEOUT;

@Slf4j
@Controller
@RequestMapping("/dataserver")
@RequiredArgsConstructor
@Validated
public class ServerController {

    private final Server server;
    private final RestTemplate restTemplate;

    private final String URI_PUSHDATA = "http://localhost:8090/hadoopserver/pushbigdata";

    @Retryable(value = {ConstraintViolationException.class}, maxAttempts = 3, backoff = @Backoff(delay = 3000))
    @PostMapping(value = "/pushdata", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> pushData(@Valid @RequestBody DataEnvelope dataEnvelope) throws HadoopClientException, IOException, NoSuchAlgorithmException {

        log.info("Data envelope received: {}", dataEnvelope.getDataHeader().getName());
        boolean checksumPass = server.saveDataEnvelope(dataEnvelope);
        log.info("Data envelope persisted. Attribute name: {}", dataEnvelope.getDataHeader().getName());

        //Exercise 5
        HttpEntity<DataEnvelope> requestEntity = new HttpEntity<>(dataEnvelope);
        ResponseEntity<HttpStatus> response = restTemplate.exchange(
                URI_PUSHDATA,
                HttpMethod.POST,
                requestEntity,
                HttpStatus.class
        );
        if (response.getStatusCode() == GATEWAY_TIMEOUT) {
            throw new HadoopClientException("Hadoop data lake is still recovering");
        }
        return ResponseEntity.ok(checksumPass);
    }

    //Exercise 3
    @GetMapping(value = "/data/{blockType}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<DataEnvelope>> getData(@Valid @PathVariable String blockType) {

        log.info("Received block type: {}", blockType);
        List<DataEnvelope> dataEnvelope = server.getDataEnvelope(BlockTypeEnum.valueOf(blockType));

        log.info("Total Data envelope retrieved: {}", dataEnvelope.size());
        return ResponseEntity.ok(dataEnvelope);
    }

    //Exercise 4
    @PutMapping(value = "/update/{name}/{newBlockType}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> updateData(@Valid @PathVariable String name, @PathVariable String newBlockType) {

        log.info("identified Using block name {}, Data envelope will be updated with block type: {} ", name, newBlockType);
        boolean updated = server.updateDataEnvelope(name, BlockTypeEnum.valueOf(newBlockType));

        log.info("Data envelope updated with block type: {}? - {}", newBlockType, updated ? "yes" : "no");
        return ResponseEntity.ok(updated);
    }

    @Recover
    public String recoverFromFailure(ConstraintViolationException cve) {
        log.info("Service recovering from the data lake");
        return "Service recovered from hadoop data lake service failure.";
    }
}
