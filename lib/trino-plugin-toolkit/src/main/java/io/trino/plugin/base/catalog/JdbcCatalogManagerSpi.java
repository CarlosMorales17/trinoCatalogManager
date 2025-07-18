/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.base.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.trino.spi.catalog.CatalogManagerSpi;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import jakarta.annotation.Nullable;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Generic JDBC‐backed implementation of {@link CatalogManagerSpi} that factors out the
 * datasource and catalog-store boilerplate so concrete implementations only need to
 * provide minimal configuration and schema-mapping logic.
 * <p>
 * Subclasses are expected to:
 * <ul>
 *     <li>Provide database connection details via {@link #createConfig(Map)}</li>
 *     <li>Define how the catalog data is mapped to their underlying tables via
 *         {@link #createSchemaMapping()}</li>
 * </ul>
 */
public abstract class JdbcCatalogManagerSpi
        implements CatalogManagerSpi
{
    protected final JdbcCatalogConfig config;
    protected final DataSource dataSource;
    protected final JdbcCatalogStore catalogStore;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JdbcSchemaMapping schemaMapping;

    protected JdbcCatalogManagerSpi(Map<String, String> properties)
    {
        this.config = createConfig(properties);
        this.dataSource = createDataSource(config);
        this.schemaMapping = createSchemaMapping();
        this.catalogStore = new JdbcCatalogStore(this.dataSource, this.schemaMapping);
    }

    /**
     * Map the provided configuration map coming from Trino to a {@link JdbcCatalogConfig}
     * instance that describes the JDBC connection and other tunables.
     */
    protected abstract JdbcCatalogConfig createConfig(Map<String, String> properties);

    /**
     * Provide database-specific DDL/DML as well as (de)serialization logic.
     */
    protected abstract JdbcSchemaMapping createSchemaMapping();

    /**
     * Hook for subclasses to customise the connection-pool settings.
     */
    protected DataSource createDataSource(JdbcCatalogConfig config)
    {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getJdbcUrl());
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());
        hikariConfig.setMaximumPoolSize(config.getMaxPoolSize());
        hikariConfig.setMinimumIdle(config.getMinIdle());
        hikariConfig.setConnectionTimeout(config.getConnectionTimeout());
        hikariConfig.setIdleTimeout(config.getIdleTimeout());
        hikariConfig.setMaxLifetime(config.getMaxLifetime());
        return new HikariDataSource(hikariConfig);
    }

    @Override
    public long getRefreshInterval()
    {
        return config.getRefreshInterval();
    }

    @Override
    public CatalogStore getCatalogStore()
    {
        return catalogStore;
    }

    @Override
    public void connect()
    {
        try {
            initializeDatabase();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to initialise JDBC catalog-store", e);
        }
    }

    @Override
    public void disconnect()
    {
        if (dataSource instanceof HikariDataSource hikari) {
            hikari.close();
        }
    }

    @Override
    public void ensureCatalogsLoaded(List<CatalogProperties> catalogsList)
    {
        // default no-op – subclasses may add custom logic/validation.
    }

    @Override
    public CatalogStore.StoredCatalog getStoredCatalog(CatalogName catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");
        return catalogStore.getCatalog(catalogName);
    }

    private void initializeDatabase()
            throws SQLException
    {
        // Execute schema SQL if supplied by mapping implementation
        List<String> ddlStatements = schemaMapping.getTableCreationStatements();
        if (ddlStatements.isEmpty()) {
            return;
        }
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            for (String ddl : ddlStatements) {
                statement.execute(ddl);
            }
        }
    }

    protected String toJson(Map<String, String> properties)
    {
        try {
            return objectMapper.writeValueAsString(properties);
        }
        catch (Exception e) {
            throw new RuntimeException("Error serialising properties to JSON", e);
        }
    }

    @SuppressWarnings("unchecked")
    protected Map<String, String> fromJson(@Nullable String json)
    {
        try {
            if (json == null || json.trim().isEmpty()) {
                return ImmutableMap.of();
            }
            return objectMapper.readValue(json, Map.class);
        }
        catch (Exception e) {
            throw new RuntimeException("Error deserialising properties from JSON", e);
        }
    }
}
