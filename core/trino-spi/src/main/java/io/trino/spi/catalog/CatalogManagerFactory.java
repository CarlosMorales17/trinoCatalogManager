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

import java.util.Map;

/**
 * Factory for creating catalog manager instances.
 *
 * This follows the same pattern as other Trino factories like ConnectorFactory
 * and CatalogStoreFactory.
 */
public interface CatalogManagerFactory
{
    /**
     * Get the name of this catalog manager.
     * This name is used in configuration to select which catalog manager to use.
     *
     * @return the catalog manager name
     */
    String getName();

    /**
     * Create a catalog manager instance with the given configuration.
     *  TODO_DC: Should this initalize the connection on startup? or should this be a lazy connection?
     *
     * @param config the configuration properties
     * @return the catalog manager instance
     */
    CatalogManagerSpi create(Map<String, String> config);
}
