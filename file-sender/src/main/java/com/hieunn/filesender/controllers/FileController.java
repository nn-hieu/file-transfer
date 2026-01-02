package com.hieunn.filesender.controllers;

import com.hieunn.filesender.services.FileService;
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
    public ResponseEntity<Void> sendFile(@RequestPart MultipartFile file, @RequestParam String targetService) {
        fileService.sendFile(file, targetService);

        return ResponseEntity.ok().build();
    }
}
