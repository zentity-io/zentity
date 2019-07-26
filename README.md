[![Build Status](https://travis-ci.org/zentity-io/zentity.svg?branch=master)](https://travis-ci.org/zentity-io/zentity)

# zentity

zentity is an **[Elasticsearch](https://www.elastic.co/products/elasticsearch)** plugin for entity resolution.

zentity aims to be:

- **Simple** - Entity resolution is hard. zentity makes it easy.
- **Fast** - Get results in real-time. From milliseconds to low seconds.
- **Generic** - Resolve anything. People, companies, locations, sessions, and more.
- **Transitive** - Resolve over multiple hops. Recursion finds dynamic identities.
- **Multi-source** - Resolve over multiple indices with disparate mappings.
- **Accommodating** - Operate on data as it exists. No changing or reindexing data.
- **Logical** - Logic is easier to read, troubleshoot, and optimize than statistics.
- **100% Elasticsearch** - Elasticsearch is a great foundation for entity resolution.


## Documentation

Documentation is hosted at [https://zentity.io/docs](https://zentity.io/docs)


## Quick start

Once you have installed Elasticsearch, you can install zentity from a remote URL or a local file.

1. Browse the [releases](https://zentity.io/releases).
2. Find a release that matches your version of Elasticsearch. Copy the name of the .zip file.
3. Install the plugin using the `elasticsearch-plugin` script that comes with Elasticsearch.

Example:

`elasticsearch-plugin install https://zentity.io/releases/zentity-1.4.0-beta1-elasticsearch-7.2.0.zip`

Read the [installation](https://zentity.io/docs/installation) docs for more details.


## Next steps

Read the [documentation](https://zentity.io/docs/basic-usage) to learn about [entity models](https://zentity.io/docs/entity-models),
how to [manage entity models](https://zentity.io/docs/rest-apis/models-api), and how to [resolve entities](https://zentity.io/docs/rest-apis/resolution-api).


## <a name="license">License</a>

```
This software is licensed under the Apache License, version 2 ("ALv2"), quoted below.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
```
