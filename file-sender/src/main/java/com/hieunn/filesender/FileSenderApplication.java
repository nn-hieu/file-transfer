package com.hieunn.filesender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class FileSenderApplication {

    public static void main(String[] args) {
        SpringApplication.run(FileSenderApplication.class, args);
    }

}
