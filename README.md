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

Documentation is hosted on the [Github Wiki](https://github.com/zentity-io/zentity/wiki). Eventually it will be
migrated to [zentity.io](https://zentity.io/).


## Quick start

Once you have installed Elasticsearch, you can install zentity from a remote URL or a local file.

### Install from remote URL

1. Browse the **[releases](https://github.com/zentity-io/zentity/releases)**.
2. Find a release that matches your version of Elasticsearch. Copy the name of the .zip file.
3. Install the plugin using the `elasticsearch-plugin` script that comes with Elasticsearch.

Example:

`elasticsearch-plugin install https://github.com/zentity-io/zentity/releases/download/zentity-0.1.1-beta.1/zentity-0.1.1-beta.1-elasticsearch-6.2.2.zip`


### Install from local file

1. Browse the **[releases](https://github.com/zentity-io/zentity/releases)**.
2. Find a release that matches your version of Elasticsearch. Download the .zip file.
4. Install the plugin using the `elasticsearch-plugin` script that comes with Elasticsearch.

Example:

`elasticsearch-plugin install file:///path/to/zentity-0.1.1-beta.1-elasticsearch-6.2.2.zip`


### Next steps

Read the **[documentation](https://github.com/zentity-io/zentity/wiki/Basic-Usage)** to learn how to create and manage
entity models and how to resolve entities.


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
