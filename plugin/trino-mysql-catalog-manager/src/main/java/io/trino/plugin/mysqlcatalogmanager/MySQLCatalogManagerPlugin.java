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

import io.airlift.log.Logger;
import io.trino.plugin.base.catalog.JdbcCatalogConfig;
import io.trino.plugin.base.catalog.JdbcCatalogData;
import io.trino.plugin.base.catalog.JdbcCatalogManagerSpi;
import io.trino.plugin.base.catalog.JdbcSchemaMapping;
import io.trino.spi.catalog.CatalogProperties;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MySQLCatalogManagerPlugin
        extends JdbcCatalogManagerSpi
{
    private static final Logger log = Logger.get(MySQLCatalogManagerPlugin.class);

    public MySQLCatalogManagerPlugin(Map<String, String> properties)
    {
        super(properties);
        log.info("MySQLCatalogManagerPlugin initialized with properties: %s", properties);
    }

    @Override
    protected JdbcCatalogConfig createConfig(Map<String, String> props)
    {
        return JdbcCatalogConfig.builder()
                .jdbcUrl(props.getOrDefault("mysql.jdbc-url", "jdbc:mysql://localhost:3306/catalogs"))
                .username(props.getOrDefault("mysql.username", "trino"))
                .password(props.getOrDefault("mysql.password", ""))
                .refreshInterval(Long.parseLong(props.getOrDefault("mysql.refresh-interval", "3000")))
                .build();
    }

    @Override
    protected JdbcSchemaMapping createSchemaMapping()
    {
        return new JdbcSchemaMapping()
        {
            @Override
            public List<String> getTableCreationStatements()
            {
                return List.of(
                        """
                        CREATE TABLE IF NOT EXISTS connectors (
                            connector_name VARCHAR(255) PRIMARY KEY,
                            properties_schema JSON NOT NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                        )""",
                        """
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
                        )"""
                );
            }


            @Override
            public String getSelectCatalogsSql()
            {
                return "SELECT catalog_name, version_identifier, catalog_config, connector_name FROM catalog_configurations";
            }

            @Override
            public String getCatalogNameColumn()
            {
                return "catalog_name";
            }

            @Override
            public JdbcCatalogData extractCatalogData(ResultSet rs) throws SQLException
            {
                String catalogName = rs.getString("catalog_name");
                String versionIdentifier = rs.getString("version_identifier");
                String catalogConfig = rs.getString("catalog_config");
                String connectorName = rs.getString("connector_name");

                Map<String, String> properties = fromJson(catalogConfig);
                // Remove metadata key if present
                properties = new HashMap<>(properties);
                properties.remove("connector.name");

                return new JdbcCatalogData(catalogName, versionIdentifier, connectorName, properties);
            }

            /*********************************************************************
             * Methods Below are "Expiremental" tos support addition of new Catalogs Throuhgh Trino
             /******************************************************************************/

            @Override
            public void bindCatalogParameters(PreparedStatement stmt, CatalogProperties catalog) throws SQLException
            {
                stmt.setString(1, catalog.catalogHandle().getCatalogName().toString());
                stmt.setString(2, catalog.catalogHandle().getVersion().toString());
                stmt.setString(3, toJson(catalog.properties()));
                stmt.setString(4, catalog.connectorName().toString());
            }

            @Override
            public String getUpsertCatalogSql()
            {
                return """
                        INSERT INTO catalog_configurations (catalog_name, version_identifier, catalog_config, connector_name)
                        VALUES (?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            catalog_config = VALUES(catalog_config),
                            connector_name = VALUES(connector_name),
                            modified_at = CURRENT_TIMESTAMP""";
            }

            @Override
            public String getDeleteCatalogSql()
            {
                return "DELETE FROM catalog_configurations WHERE catalog_name = ?";
            }
        };
    }

    @Override
    public void ensureCatalogsLoaded(List<CatalogProperties> catalogs)
    {
        // Optional: emit some diagnostics
        log.debug("ensureCatalogsLoaded invoked with %d catalogs", catalogs.size());
    }
}
