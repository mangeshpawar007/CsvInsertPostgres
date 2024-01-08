package com.postgres.postgres.controller;

import com.postgres.postgres.service.CsvService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CsvController {

    @Autowired
    CsvService csvService;

    @PostMapping("/ingest")
    public void ingestCsv() {
        csvService.ingestData();
    }
}
