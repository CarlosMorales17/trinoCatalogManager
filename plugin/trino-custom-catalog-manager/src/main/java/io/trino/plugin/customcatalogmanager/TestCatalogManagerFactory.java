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
package io.trino.plugin.customcatalogmanager;

import io.airlift.log.Logger;
import io.trino.spi.catalog.CatalogManagerFactory;
import io.trino.spi.catalog.CatalogManagerSpi;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class TestCatalogManagerFactory
        implements CatalogManagerFactory
{
    private static final Logger log = Logger.get(TestCatalogManagerFactory.class);

    @Override
    public String getName()
    {
        return "test-catalog-manager";
    }

    @Override
    public CatalogManagerSpi create(Map<String, String> config)
    {
        requireNonNull(config, "config is null");
        log.info("🚀 Creating TestCatalogManager with config: %s", config);
        return new TestCatalogManager(config);
    }
}
