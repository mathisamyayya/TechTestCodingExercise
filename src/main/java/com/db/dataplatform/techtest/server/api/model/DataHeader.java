package com.db.dataplatform.techtest.server.api.model;

import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import java.io.Serializable;

@JsonSerialize(as = DataHeader.class)
@JsonDeserialize(as = DataHeader.class)
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class DataHeader implements Serializable {

    @NotBlank
    private String name;

    private BlockTypeEnum blockType;

}
