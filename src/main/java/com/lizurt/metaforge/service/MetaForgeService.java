package com.lizurt.metaforge.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.sql.*;

@Service
@RequiredArgsConstructor
public class MetaForgeService {

    private final PrimaryKeyFixerService primaryKeyFixerService;

    public void fixDatabase(String dbUrl, String dbUser, String dbPassword) {
        // todo: fix domains firstly
        primaryKeyFixerService.fixPrimaryKeys(dbUrl, dbUser, dbPassword);
    }
}
