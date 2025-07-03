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
package io.trino.spi.catalog;

import java.util.List;

/**
 * SPI interface for custom catalog managers.
 * Implementations of this interface can replace the default catalog management logic
 * in Trino to provide custom catalog storage and management capabilities.
 *
 * The interface provides methods for:
 * - Loading initial catalogs from external sources
 * - Retrieving catalog names and properties
 * - Creating and dropping catalogs
 * - Managing catalog lifecycle
 *
 * Custom implementations can integrate with databases, REST APIs, configuration management
 * systems, or any other external catalog source.
 *
 * RECOMMENDED APPROACH:
 * Implement this interface and leverage the existing CatalogStore infrastructure
 * by providing a custom CatalogStore implementation through getCatalogStore().
 * This allows you to reuse Trino's existing catalog management logic while
 * providing your own storage backend.
 */
public interface CatalogManagerSpi
{
    /**
     * Returns the refresh interval in milliseconds for catalog synchronization.
     * Return 0 to disable automatic refresh.
     *
     * @return refresh interval in milliseconds
     */
    default long getRefreshInterval()
    {
        return 0;
    }

    /**
     * Returns the catalog store used by this catalog manager.
     * This is the RECOMMENDED way to implement custom catalog management.
     * Provide a custom CatalogStore implementation that connects to your
     * external catalog source (database, REST API, etc.).
     *
     * @return the catalog store instance
     */
    CatalogStore getCatalogStore();

    /**
     * Establishes connection to the external catalog source.
     * This method is called during catalog manager initialization.
     */
    void connect();

    /**
     * Closes connection to the external catalog source.
     * This method is called during catalog manager shutdown.
     */
    void disconnect();

    /**
     * This an Example Of how we can leave extensibilty for the user to extend thier logic
     *
     * @param catalogsList the list of catalogs to ensure are loaded
     */
    void ensureCatalogsLoaded(List<CatalogProperties> catalogsList);
}
