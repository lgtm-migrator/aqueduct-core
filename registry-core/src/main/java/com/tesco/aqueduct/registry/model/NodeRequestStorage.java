package com.tesco.aqueduct.registry.model;

import java.sql.SQLException;

public interface NodeRequestStorage {
    void save(NodeRequest nodeRequest) throws SQLException;
    BootstrapType requiresBootstrap(Node node) throws SQLException;
}
