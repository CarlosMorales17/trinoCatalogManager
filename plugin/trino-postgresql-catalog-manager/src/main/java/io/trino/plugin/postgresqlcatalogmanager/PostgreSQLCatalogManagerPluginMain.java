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
package io.trino.plugin.postgresqlcatalogmanager;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.catalog.CatalogManagerFactory;

/**
 * PostgreSQL catalog manager plugin that provides persistent storage for catalog definitions.
 * This plugin stores catalog information in a PostgreSQL database with proper CRUD operations.
 */
public final class PostgreSQLCatalogManagerPluginMain
        implements Plugin
{
    @Override
    public Iterable<CatalogManagerFactory> getCatalogManagerFactories()
    {
        return ImmutableList.of(new PostgreSQLCatalogManagerFactory());
    }
}
