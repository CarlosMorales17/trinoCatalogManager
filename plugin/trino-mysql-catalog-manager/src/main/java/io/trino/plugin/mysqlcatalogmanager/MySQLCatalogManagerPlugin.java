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
package io.trino.plugin.mysqlcatalogmanager;

import com.google.common.collect.ImmutableMap;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;
import io.trino.spi.catalog.CatalogManagerSpi;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorName;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static java.util.Objects.requireNonNull;

/**
 * MySQL-based catalog manager that stores catalog information in a MySQL database.
 * This provides persistent storage for catalog definitions with proper CRUD operations.
 */
public final class MySQLCatalogManagerPlugin
        implements CatalogManagerSpi
{
    private static final Logger log = Logger.get(MySQLCatalogManagerPlugin.class);

    private final long refreshInterval;
    private final MySQLCatalogStore catalogStore;
    private final DataSource dataSource;

    public MySQLCatalogManagerPlugin(Map<String, String> config)
    {
        requireNonNull(config, "config is null");
        log.info("MySQLCatalogManagerPlugin initializing with config: %s", config);

        // Configuration
        String jdbcUrl = config.getOrDefault("mysql.jdbc-url", "jdbc:mysql://localhost:3306/catalogs");
        String username = config.getOrDefault("mysql.username", "trino");
        String dbAuth = config.getOrDefault("mysql.password", "");
        this.refreshInterval = Long.parseLong(config.getOrDefault("mysql.refresh-interval", "30000"));

        // Create HikariCP data source
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(dbAuth);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setMaxLifetime(1800000);

        this.dataSource = new HikariDataSource(hikariConfig);
        this.catalogStore = new MySQLCatalogStore(dataSource);

        log.info("MySQLCatalogManagerPlugin initialized successfully!");
    }

    @Override
    public long getRefreshInterval()
    {
        return refreshInterval;
    }

    @Override
    public CatalogStore getCatalogStore()
    {
        return catalogStore;
    }

    @Override
    public void connect()
    {
        log.info("MySQLCatalogManagerPlugin connecting to database...");
        try {
            catalogStore.initializeDatabase();
            log.info("MySQLCatalogManagerPlugin connected successfully!");
        }
        catch (SQLException e) {
            log.error(e, "Failed to connect to MySQL database");
            throw new RuntimeException("Failed to connect to MySQL database", e);
        }
    }

    @Override
    public void disconnect()
    {
        log.info("MySQLCatalogManagerPlugin disconnecting...");
        try {
            if (dataSource instanceof HikariDataSource) {
                ((HikariDataSource) dataSource).close();
            }
            log.info("MySQLCatalogManagerPlugin disconnected successfully!");
        }
        catch (Exception e) {
            log.error(e, "Error during disconnect");
        }
    }

    @Override
    public void ensureCatalogsLoaded(List<CatalogProperties> catalogsList)
    {
        requireNonNull(catalogsList, "catalogs is null");

        log.info("ensureCatalogsLoaded() called with %d catalogs", catalogsList.size());
        for (CatalogProperties catalog : catalogsList) {
            log.info("Ensuring catalog loaded: %s (%s)",
                    catalog.catalogHandle().getCatalogName(),
                    catalog.connectorName());
        }
    }

    @Override
    public CatalogStore.StoredCatalog getStoredCatalog(CatalogName catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");
        log.info("Getting stored catalog: %s", catalogName);

        return catalogStore.getCatalogs().stream()
                .filter(storedCatalog -> storedCatalog.name().equals(catalogName))
                .findFirst()
                .orElse(null);
    }

    /**
     * MySQL-based implementation of CatalogStore.
     * Stores catalog information in a MySQL database with proper CRUD operations.
     */
    private static class MySQLCatalogStore
            implements CatalogStore
    {
        private static final Logger log = Logger.get(MySQLCatalogStore.class);

        private final DataSource dataSource;
        private final AtomicLong versionCounter = new AtomicLong(System.currentTimeMillis());

        public MySQLCatalogStore(DataSource dataSource)
        {
            this.dataSource = requireNonNull(dataSource, "dataSource is null");
        }

        public void initializeDatabase()
                throws SQLException
        {
            log.info("Initializing MySQL database schema...");

            try (Connection connection = dataSource.getConnection();
                    Statement statement = connection.createStatement()) {
                // Create connectors table
                statement.execute("""
                    CREATE TABLE IF NOT EXISTS connectors (
                        connector_name VARCHAR(255) PRIMARY KEY,
                        properties_schema JSON NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    )
                    """);

                // Create catalog_configurations table
                statement.execute("""
                    CREATE TABLE IF NOT EXISTS catalog_configurations (
                        catalog_name VARCHAR(255) NOT NULL,
                        version_identifier VARCHAR(255) NOT NULL,
                        org_id VARCHAR(255),
                        catalog_config JSON,
                        connector_name VARCHAR(255),
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        PRIMARY KEY (catalog_name, version_identifier),
                        FOREIGN KEY (connector_name) REFERENCES connectors(connector_name)
                    )
                    """);

                log.info("Database schema initialized successfully");

                // Create initial connectors and catalogs if none exist
                if (getConnectorCount() == 0) {
                    createInitialConnectors();
                }
                if (getCatalogCount() == 0) {
                    createInitialCatalogs();
                }
            }
        }

        private void createInitialConnectors()
        {
            log.info("Creating initial connectors...");

            // Create TPCH connector
            insertConnector("tpch", "{\"connector.name\": \"string\"}");

            // Create TPCDS connector
            insertConnector("tpcds", "{\"connector.name\": \"string\"}");

            // Create Memory connector
            insertConnector("memory", "{\"connector.name\": \"string\", \"memory.max-data-per-node\": \"string\"}");

            log.info("Initial connectors created successfully");
        }

        private void insertConnector(String connectorName, String propertiesSchema)
        {
            String sql = """
                INSERT INTO connectors (connector_name, properties_schema)
                VALUES (?, ?)
                ON DUPLICATE KEY UPDATE
                    properties_schema = VALUES(properties_schema),
                    modified_at = CURRENT_TIMESTAMP
                """;

            try (Connection connection = dataSource.getConnection();
                    PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setString(1, connectorName);
                statement.setString(2, propertiesSchema);
                statement.executeUpdate();
            }
            catch (SQLException e) {
                log.error(e, "Error creating connector %s", connectorName);
                throw new RuntimeException("Error creating connector", e);
            }
        }

        private void createInitialCatalogs()
        {
            log.info("Creating initial test catalogs...");

            addOrReplaceCatalog(createCatalogProperties(
                new CatalogName("mysql_memory"),
                new ConnectorName("memory"),
                ImmutableMap.of("connector.name", "memory", "memory.max-data-per-node", "128MB")));

            addOrReplaceCatalog(createCatalogProperties(
                new CatalogName("mysql_tpch"),
                new ConnectorName("tpch"),
                ImmutableMap.of("connector.name", "tpch")));

            addOrReplaceCatalog(createCatalogProperties(
                new CatalogName("mysql_tpcds"),
                new ConnectorName("tpcds"),
                ImmutableMap.of("connector.name", "tpcds")));

            log.info("Initial catalogs created successfully");
        }

        private long getConnectorCount()
        {
            try (Connection connection = dataSource.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM connectors")) {

                if (resultSet.next()) {
                    return resultSet.getLong(1);
                }
                return 0;
            }
            catch (SQLException e) {
                log.error(e, "Error counting connectors");
                return 0;
            }
        }

        private long getCatalogCount()
        {
            try (Connection connection = dataSource.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM catalog_configurations")) {

                if (resultSet.next()) {
                    return resultSet.getLong(1);
                }
                return 0;
            }
            catch (SQLException e) {
                log.error(e, "Error counting catalogs");
                return 0;
            }
        }

        @Override
        public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
        {
            log.info("Creating catalog properties for %s", catalogName);

            String version = String.valueOf(versionCounter.incrementAndGet());
            CatalogHandle catalogHandle = createRootCatalogHandle(catalogName, new CatalogHandle.CatalogVersion(version));

            return new CatalogProperties(catalogHandle, connectorName, properties);
        }

        @Override
        public void addOrReplaceCatalog(CatalogProperties catalogProperties)
        {
            log.info("Storing catalog %s to MySQL database", catalogProperties.catalogHandle().getCatalogName());

            String sql = """
                INSERT INTO catalog_configurations (catalog_name, version_identifier, catalog_config, connector_name)
                VALUES (?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    catalog_config = VALUES(catalog_config),
                    connector_name = VALUES(connector_name),
                    modified_at = CURRENT_TIMESTAMP
                """;

            try (Connection connection = dataSource.getConnection();
                    PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setString(1, catalogProperties.catalogHandle().getCatalogName().toString());
                statement.setString(2, catalogProperties.catalogHandle().getVersion().toString());
                statement.setString(3, serializePropertiesToJson(catalogProperties.properties()));
                statement.setString(4, catalogProperties.connectorName().toString());

                statement.executeUpdate();
                log.info("Successfully stored catalog %s", catalogProperties.catalogHandle().getCatalogName());
            }
            catch (SQLException e) {
                log.error(e, "Error storing catalog %s", catalogProperties.catalogHandle().getCatalogName());
                throw new RuntimeException("Error storing catalog", e);
            }
        }

        @Override
        public void removeCatalog(CatalogName catalogName)
        {
            log.info("Removing catalog %s from MySQL database", catalogName);

            String sql = "DELETE FROM catalog_configurations WHERE catalog_name = ?";

            try (Connection connection = dataSource.getConnection();
                    PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setString(1, catalogName.toString());
                int rowsAffected = statement.executeUpdate();

                if (rowsAffected > 0) {
                    log.info("Successfully removed catalog %s", catalogName);
                }
                else {
                    log.warn("Catalog %s not found for removal", catalogName);
                }
            }
            catch (SQLException e) {
                log.error(e, "Error removing catalog %s", catalogName);
                throw new RuntimeException("Error removing catalog", e);
            }
        }

        @Override
        public Collection<StoredCatalog> getCatalogs()
        {
            String sql = "SELECT catalog_name, version_identifier, catalog_config, connector_name FROM catalog_configurations";
            List<StoredCatalog> catalogs = new ArrayList<>();

            try (Connection connection = dataSource.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(sql)) {

                while (resultSet.next()) {
                    String catalogName = resultSet.getString("catalog_name");
                    String versionIdentifier = resultSet.getString("version_identifier");
                    String catalogConfig = resultSet.getString("catalog_config");
                    String connectorName = resultSet.getString("connector_name");

                    catalogs.add(new MySQLStoredCatalog(catalogName, versionIdentifier, catalogConfig, connectorName));
                }

                log.info("Loaded %d catalogs from MySQL database", catalogs.size());
                return catalogs;
            }
            catch (SQLException e) {
                log.error(e, "Error loading catalogs from database");
                throw new RuntimeException("Error loading catalogs", e);
            }
        }

        private String serializePropertiesToJson(Map<String, String> properties)
        {
            if (properties == null || properties.isEmpty()) {
                return "{}";
            }

            StringBuilder json = new StringBuilder();
            json.append("{");
            boolean first = true;
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (!first) {
                    json.append(", ");
                }
                json.append("\"").append(escapeJsonString(entry.getKey())).append("\": \"")
                    .append(escapeJsonString(entry.getValue())).append("\"");
                first = false;
            }
            json.append("}");
            return json.toString();
        }

        private String escapeJsonString(String str)
        {
            if (str == null) {
                return "";
            }
            return str.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
        }

        private Map<String, String> deserializePropertiesFromJson(String json)
        {
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            if (json != null && !json.trim().isEmpty() && !json.equals("{}")) {
                // Simple JSON parsing - for production use, consider using a proper JSON library
                json = json.trim();
                if (json.startsWith("{") && json.endsWith("}")) {
                    json = json.substring(1, json.length() - 1);
                    String[] pairs = json.split(",\\s*");
                    for (String pair : pairs) {
                        String[] keyValue = pair.split(":\\s*", 2);
                        if (keyValue.length == 2) {
                            String key = keyValue[0].trim().replaceAll("^\"|\"$", "");
                            String value = keyValue[1].trim().replaceAll("^\"|\"$", "");
                            builder.put(key, value);
                        }
                    }
                }
            }
            return builder.buildOrThrow();
        }

        /**
         * MySQL-specific implementation of StoredCatalog.
         */
        private class MySQLStoredCatalog
                implements StoredCatalog
        {
            private final CatalogName catalogName;
            private final String versionIdentifier;
            private final ConnectorName connectorName;
            private final Map<String, String> properties;

            public MySQLStoredCatalog(String catalogName, String versionIdentifier, String catalogConfig, String connectorName)
            {
                this.catalogName = new CatalogName(catalogName);
                this.versionIdentifier = versionIdentifier;
                this.connectorName = new ConnectorName(connectorName);
                this.properties = deserializePropertiesFromJson(catalogConfig);
            }

            @Override
            public CatalogName name()
            {
                return catalogName;
            }

            @Override
            public CatalogProperties loadProperties()
            {
                CatalogHandle catalogHandle = createRootCatalogHandle(
                    catalogName,
                    new CatalogHandle.CatalogVersion(versionIdentifier));

                // Filter out connector.name property - it's metadata for Trino, not a connector configuration
                Map<String, String> filteredProperties = new HashMap<>(properties);
                filteredProperties.remove("connector.name");

                return new CatalogProperties(catalogHandle, connectorName, ImmutableMap.copyOf(filteredProperties));
            }
        }
    }
}
