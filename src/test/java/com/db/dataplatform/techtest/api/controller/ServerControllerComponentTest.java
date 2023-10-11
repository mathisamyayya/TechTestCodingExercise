package com.db.dataplatform.techtest.api.controller;

import com.db.dataplatform.techtest.TestDataHelper;
import com.db.dataplatform.techtest.server.api.controller.ServerController;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

@RunWith(MockitoJUnitRunner.class)
public class ServerControllerComponentTest {

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

    @Mock
    private Server serverMock;

    @Mock
    private RestTemplate restTemplate;

    private DataEnvelope testDataEnvelope;
    private List<DataEnvelope> testDataEnvelopes;
    private ObjectMapper objectMapper;
    private MockMvc mockMvc;
    private ServerController serverController;

    @Before
    public void setUp() throws HadoopClientException, NoSuchAlgorithmException, IOException {
        serverController = new ServerController(serverMock, restTemplate);
        mockMvc = standaloneSetup(serverController).build();
        objectMapper = Jackson2ObjectMapperBuilder
                .json()
                .build();

        testDataEnvelope = TestDataHelper.createTestDataEnvelopeApiObject();
        testDataEnvelopes = Collections.singletonList(testDataEnvelope);

        when(serverMock.saveDataEnvelope(any(DataEnvelope.class))).thenReturn(true);
        when(serverMock.getDataEnvelope(any(BlockTypeEnum.class))).thenReturn(testDataEnvelopes);
        when(serverMock.updateDataEnvelope(anyString(), any(BlockTypeEnum.class))).thenReturn(true);
        when(restTemplate.exchange(any(String.class), any(HttpMethod.class), any(), any(Class.class))).thenReturn(new ResponseEntity<>(HttpStatus.OK));
    }

    @Test
    public void testPushDataPostCallWorksAsExpected() throws Exception {

        String testDataEnvelopeJson = objectMapper.writeValueAsString(testDataEnvelope);

        MvcResult mvcResult = mockMvc.perform(post(URI_PUSHDATA)
                        .content(testDataEnvelopeJson)
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk())
                .andReturn();

        boolean checksumPass = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
        assertThat(checksumPass).isTrue();
    }

    @Test
    public void testGetDataGetCallWorksAsExpected() throws Exception {

        String testDataEnvelopesJson = objectMapper.writeValueAsString(testDataEnvelopes);

        MvcResult mvcResult = mockMvc.perform(get(URI_GETDATA.expand(BlockTypeEnum.BLOCKTYPEA.name()).toString()))
                .andExpect(status().isOk())
                .andReturn();

        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo(testDataEnvelopesJson);
    }

    @Test
    public void testUpdateDataPutCallWorksAsExpected() throws Exception {

        MvcResult mvcResult = mockMvc.perform(put(URI_PATCHDATA.expand("TSLA-USDGBP-10Y", BlockTypeEnum.BLOCKTYPEA.name()).toString()))
                .andExpect(status().isOk())
                .andReturn();

        boolean checksumPass = Boolean.parseBoolean(mvcResult.getResponse().getContentAsString());
        assertThat(checksumPass).isTrue();
    }
}
