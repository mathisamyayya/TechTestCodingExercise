package com.db.dataplatform.techtest.service;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.component.impl.ServerImpl;
import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.mapper.ServerMapperConfiguration;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.modelmapper.ModelMapper;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.db.dataplatform.techtest.TestDataHelper.createTestDataEnvelopeApiObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ServerServiceTests {

    @Mock
    private DataBodyService dataBodyServiceImplMock;

    private ModelMapper modelMapper;

    private DataBodyEntity expectedDataBodyEntity;

    private List<DataBodyEntity> dataEnvelopes;
    private Optional<DataBodyEntity> optionalDataBodyEntity;
    private DataEnvelope testDataEnvelope;

    private Server server;

    @Before
    public void setup() {
        ServerMapperConfiguration serverMapperConfiguration = new ServerMapperConfiguration();
        modelMapper = serverMapperConfiguration.createModelMapperBean();

        testDataEnvelope = createTestDataEnvelopeApiObject();
        expectedDataBodyEntity = modelMapper.map(testDataEnvelope.getDataBody(), DataBodyEntity.class);
        expectedDataBodyEntity.setDataHeaderEntity(modelMapper.map(testDataEnvelope.getDataHeader(), DataHeaderEntity.class));

        server = new ServerImpl(dataBodyServiceImplMock, modelMapper);

        dataEnvelopes = Collections.singletonList(expectedDataBodyEntity);
        optionalDataBodyEntity = Optional.of(expectedDataBodyEntity);
        when(dataBodyServiceImplMock.getDataByBlockType(any(BlockTypeEnum.class))).thenReturn(dataEnvelopes);
        when(dataBodyServiceImplMock.getDataByBlockName(anyString())).thenReturn(optionalDataBodyEntity);
    }

    @Test
    public void shouldSaveDataEnvelopeAsExpected() throws NoSuchAlgorithmException, IOException {
        boolean success = server.saveDataEnvelope(testDataEnvelope);

        assertThat(success).isTrue();
        verify(dataBodyServiceImplMock, times(1)).saveDataBody(eq(expectedDataBodyEntity));
    }

    @Test
    public void shouldGetListOfDataEnvelopeAsExpected() {
        List<DataEnvelope> dataEnvelope = server.getDataEnvelope(BlockTypeEnum.BLOCKTYPEA);

        assertThat(dataEnvelope).isNotEmpty();
        assertThat(dataEnvelope.get(0).getDataBody().getDataBody()).isEqualTo(dataEnvelopes.get(0).getDataBody());
        verify(dataBodyServiceImplMock, times(1)).getDataByBlockType(BlockTypeEnum.BLOCKTYPEA);
    }

    @Test
    public void shouldUpdateDataEnvelopeAsExpected() {
        boolean updated = server.updateDataEnvelope("TSLA-USDGBP-10Z", BlockTypeEnum.BLOCKTYPEA);

        assertThat(updated).isTrue();
        verify(dataBodyServiceImplMock, times(1)).getDataByBlockName("TSLA-USDGBP-10Z");
    }

    @Test(expected = HadoopClientException.class)
    public void shouldCallHadoopDataLakeServiceAsExpected() throws HadoopClientException {
        RestTemplate restTemplate = spy(new RestTemplate());
        String url = "http://localhost:8090/hadoopserver/pushbigdata";
        server.callHadoopDataLakeService(restTemplate, url, testDataEnvelope);

        verify(restTemplate, times(1)).exchange(url, HttpMethod.POST, any(HttpEntity.class), HttpStatus.class);
    }
}
