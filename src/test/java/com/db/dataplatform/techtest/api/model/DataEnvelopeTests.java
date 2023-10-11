package com.db.dataplatform.techtest.api.model;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static com.db.dataplatform.techtest.TestDataHelper.DUMMY_DATA;
import static com.db.dataplatform.techtest.TestDataHelper.TEST_NAME;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class DataEnvelopeTests {

    @Test
    public void assignDataHeaderFieldsShouldWorkAsExpected() {
        String MD5_CHECKSUM = "cecfd3953783df706878aaec2c22aa70";
        DataHeader dataHeader = new DataHeader(TEST_NAME, BlockTypeEnum.BLOCKTYPEA);
        DataBody dataBody = new DataBody(DUMMY_DATA);

        DataEnvelope dataEnvelope = new DataEnvelope(MD5_CHECKSUM, dataHeader, dataBody);

        assertThat(dataEnvelope).isNotNull();
        assertThat(dataEnvelope.getDataHeader()).isNotNull();
        assertThat(dataEnvelope.getDataBody()).isNotNull();
        assertThat(dataEnvelope.getDataHeader()).isEqualTo(dataHeader);
        assertThat(dataEnvelope.getDataHeader()).isEqualTo(dataHeader);
        assertThat(dataBody.getDataBody()).isEqualTo(DUMMY_DATA);
    }
}
