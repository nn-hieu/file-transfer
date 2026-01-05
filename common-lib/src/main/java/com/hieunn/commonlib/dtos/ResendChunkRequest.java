package com.hieunn.commonlib.dtos;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ResendChunkRequest {
    private FileMetadata metadata;
    private int[] indexes;
}
