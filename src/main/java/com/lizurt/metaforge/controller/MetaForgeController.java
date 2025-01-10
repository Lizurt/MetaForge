package com.lizurt.metaforge.controller;

import com.lizurt.metaforge.service.MetaForgeService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class MetaForgeController {
    private final MetaForgeService metaForgeService;

    @PutMapping("/fix")
    public void fixDatabase(@RequestBody String dbUrl, @RequestBody String dbUser, @RequestBody String dbPassword) {
        metaForgeService.fixDatabase(dbUrl, dbUser, dbPassword);
    }
}
