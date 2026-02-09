package com.hieunn.filesender.controllers;

import com.hieunn.filesender.services.FileService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/files")
@RequiredArgsConstructor
public class FileController {
    private final FileService fileService;

    @PostMapping(
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    @Operation(
            summary = "API Send file",
            description = """
                    Send your file to another service through MQTT Broker (Mosquitto)
                    """
    )
    public ResponseEntity<Void> sendFile(
            @RequestPart
            @Parameter(
                    description = "Your file",
                    required = true,
                    content = @Content(mediaType = MediaType.APPLICATION_OCTET_STREAM_VALUE)
            )
            MultipartFile file,

            @RequestParam
            @Parameter(
                    description = "Name of the service you want to send file to",
                    example = "file-receiver",
                    required = true
            )
            String targetService
    ) {
        fileService.sendFile(file, targetService);

        return ResponseEntity.ok().build();
    }
}
