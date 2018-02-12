# zentity

zentity is an [Elasticsearch](https://www.elastic.co/products/elasticsearch) plugin for real-time entity
resolution. It aims to be:

- **Simple** - Entity resolution is hard. zentity offers a framework to make it easy.
- **Fast** - Get results at real-time interactive speeds.
- **Logical** - Logic is easier to read, troubleshoot, and optimize than black box statistical algorithms.
- **Transitive** - Resolve entities that span multiple hops of connections.
- **Multi-source** - Resolve entities that span multiple indices with disparate mappings.
- **Accommodating** - Operate on data as it exists. Avoid reindexing or ETL.
- **100% Elasticsearch** - Elasticsearch has a great foundation for real-time entity resolution.


---

## Contents

1. [Getting started](#getting-started)
2. [Basic usage](#basic-usage)
3. [Entity models](#entity-models)
   - [Usage](#entity-models.usage)
   - [Specification](#entity-models.specification)
   - [Tips](#entity-models.tips)
4. [REST APIs](#rest-apis)
   - [Resolution API](#resolution-api)
   - [Models API](#models-api)
5. [License](#license)

---


## <a name="getting-started">Getting started</a>


#### Step 1. Install Elasticsearch

Download: [https://www.elastic.co/downloads/elasticsearch](https://www.elastic.co/downloads/elasticsearch)


#### Step 2. Install zentity

`elasticsearch-plugin install zentity`


#### Step 3. Verify installation

**Example request:**

`GET http://localhost:9200/_zentity`

**Example response:**

```javascript
{
  "name": "zentity",
  "description": "Real-time entity resolution for Elasticsearch.",
  "version": "0.1",
  "author": "Dave Moore",
  "website": "http://zentity.io"
}
```


## <a name="basic-usage">Basic usage</a>


### Step 1. Index some data.

zentity operates on data that is indexed in ***[Elasticsearch](#https://www.elastic.co/products/elasticsearch)***,
an open source search engine for real-time search and analytics at scale. The most common tools for indexing
documents in Elasticsearch are [Logstash](https://www.elastic.co/guide/en/logstash/6.1/introduction.html) and
[Beats](https://www.elastic.co/guide/en/beats/libbeat/current/beats-reference.html). You can also index single
documents using the [Index API](https://www.elastic.co/guide/en/elasticsearch/guide/current/index-doc.html) or
[Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html).


### Step 2. Define an entity model.

***[Entity models](#entity-models)*** are the most important constructs you need to learn about. zentity uses entity
models to construct queries, match attributes across disparate indices, and resolve entities.

An *entity model* defines the logic for resolving an *entity type* such as a person or organization. It defines the
attributes of the entity ([`"attributes"`](#entity-models.attributes)), the logic to match each attribute
([`"matchers"`](#entity-models.attributes.matcher)), the logic to resolve documents to an entity based on the
matching attributes ([`"resolvers"`](#entity-models.resolvers)), and the associations between attribute matchers and
index fields ([`"indices"`](#entity-models.indices)). This is the step that demands the most thinking. You need to
think about what attributes constitute an entity type, what logic goes into matching each attribute, what "attribute
matchers" map to what indexed fields, and what combinations of matched attributes lead to resolution.

Luckily, all this thinking will pay off quickly, because entity models have two great features:

**Reusability**

Once you have an entity model you can use it everywhere. As you index new data sets with fields that map to familiar
attributes, you can include them in your entity resolution jobs. If you index data with new attributes that aren't
already in your model, you can simply update your model to support them.

**Flexibility**

You don't need to change your data to use an entity model. An entity model only controls the execution of queries.
So there's no risk in updating or experimenting with an entity model.


### Step 3. Submit a resolution request.

So you have some data and an entity model. Now you can resolve entities!

Once you have an [entity model](#entity-models), you can use the ***[Resolution API](#resolution-api)*** to run an
entity resolution job using some input.

**Example**

Run an entity resolution job using an indexed entity model called `person`.

```javascript
POST _zentity/resolution/person?pretty
{
  "attributes": {
    "name": "Alice Jones",
    "dob": "1984-01-01",
    "phone": [ "555-123-4567", "555-987-6543" ]
  }
}
```

Run an entity resolution job using an embeded entity model. This example uses three attributes (each with two
matchers), two indices, and two resolvers.

```javascript
POST _zentity/resolution?pretty
{
  "attributes": {
    "name": "Alice Jones",
    "dob": "1984-01-01",
    "phone": [ "555-123-4567", "555-987-6543" ]
  },
  "model": {
    "attributes": {
        "name": {
          "text": {
            "match": {
              "{{ field }}": {
                "query": "{{ value }}",
                "fuzziness": 2
              }
            }
          },
          "phonetic": {
            "match": {
              "{{ field }}": {
                "query": "{{ value }}",
                "fuzziness": 0
              }
            }
          }
        },
        "dob": {
          "text": {
            "match": {
              "{{ field }}": "{{ value }}"
            }
          },
          "keyword": {
            "term": {
              "{{ field }}": "{{ value }}"
            }
          }
        },
        "phone": {
          "text": {
            "match": {
              "{{ field }}": "{{ value }}"
            }
          },
          "keyword": {
            "term": {
              "{{ field }}": "{{ value }}"
            }
          }
        }
    },
    "indices": {
      "foo_index": {
        "name.text": "full_name",
        "name.phonetic": "full_name.phonetic",
        "dob.keyword": "date_of_birth.keyword",
        "phone.keyword": "telephone.keyword"
      },
      "bar_index": {
        "name.text": "nm",
        "dob.text": "db",
        "phone.text": "ph"
      }
    },
    "resolvers": {
      "name_dob": [ "name", "dob" ],
      "name_phone": [ "name", "phone" ]
    }
  }
}
```


## <a name="entity-models">Entity models</a>

zentity relies on objects called ***entity models*** to control the execution of entity resolution jobs.
Entity models serve three purposes:

1. They define how to match each of the [`"attributes"`](#entity-models.attributes) of the entity.
2. They define how attributes map to the fields of [`"indices"`](#entity-models.indices) in Elasticsearch.
3. They define which combinations of [`"attributes"`](#entity-models.attributes) lead to resolution.


### <a name="entity-models.usage">Usage</a>

You must provide an entity model when making a resolution request. You can provide it in two ways:

- Option 1. You can embed the entity model in the request under a field called `"model"`.
- Option 2. You can index the entity model using the [Models API](#models-api) and reference it by its `entity_type`.

Option (2) gives you the ability to share, reuse, and build upon existing entity models. You can manage entity models
using the [Models API](#models-api). zentity stores entity models in an Elasticsearch index called `.zentity-models`.
Each document in this index represents the entity model for a distinct entity type. The entity type is listed in the
[`_id`](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-id-field.html) field of the document.
There can be only one entity model for a given entity type. Once you have indexed an entity model, you can use it by
setting the `entity_type` parameter in your requests.


### <a name="entity-models.specification">Specification</a>

Let's look at what's inside an entity model.

```javascript
{
  "attributes": {
    ATTRIBUTE: {
      MATCHER: QUERY_TEMPLATE,
      ...
    },
    ...
  },
  "indices": {
    INDEX: {
      ATTRIBUTE.MATCHER: FIELD,
      ...
    },
    ...
  },
  "resolvers": {
    RESOLVER: [
      ATTRIBUTE,
      ...
    ],
    ...
  }
}
```

An entity model has three required objects: [`"attributes"`](#entity-models.attributes),
[`"indices"`](#entity-models.indices), [`"resolvers"`](#entity-models.resolvers).


#### <a name="entity-models.attributes">`"attributes"`</a>

Attributes are elements that can assist the resolution of entities. For example, some common attributes of a person
include name, date of birth, and phone number. Each attribute has its own particular data qualities and purposes in the
real world. Therefore, zentity matches the values of each attribute using logic that is distinct to each attribute.

Some attributes can be matched using different methods. For example, a name could be matched by its exact value or its
phonetic value. Therefore the entity model allows each attribute to have one or more "matchers." A matcher is simply a
clause of a [`"bool"` query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html)
in Elasticsearch. This means that if *any* matcher of an attribute yields a match for a given value, then the attribute
will be considered a match regardless of the results of the other matchers.

<a name="entity-models.attributes.attribute"></a>
- **`ATTRIBUTE`** - The distinct name of an attribute. Some examples could include `"name"`, `"dob"`, `"phone"`, etc.
The name cannot include periods. The values of the attribute are one or more `MATCHER` objects.

<a name="entity-models.attributes.matcher"></a>
- **`MATCHER`** - The distinct name of a matcher. Each matcher represents one valid method for matching an attribute.
The name cannot include periods. The value of the matcher is a `QUERY_TEMPLATE` object.

<a name="entity-models.attributes.query-template"></a>
- **`QUERY_TEMPLATE`** - An object that represents the clause of a [`"bool"` query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html)
in Elasticsearch. Each query template will be stitched together in a single `"bool"` query, so it must follow the
correct syntax for a `"bool"` query clause, except you don't need to include the top-level field `"bool"` or its
subfields such as `"must"` or `"should"`. The query template uses Mustache syntax to pass two important variables:
**`{{ field }}`** and **`{{ value }}`**. The `field` variable will be populated with the index field that maps to the
attribute. The `value` field will be populated with the value that will be queried for that attribute.


#### <a name="entity-models.indices">`"indices"`</a>

Different indices in Elasticsearch might have data that can be matched as attributes, but each index might use slightly
different field names or data types for the same data. Therefore, zentity uses a map to translate the different field
names to the attributes of our entity model.

The entity model maps attribute matchers to index fields. Remember how each attribute can be matched in different ways,
such as a name that can be matched by its exact value or its phonetic value? Elasticsearch would index those different
values as distinct fields, such as `"name.keyword"` and `"name.phonetic"`. This is why the entity model maps attribute
matchers -- not just attributes -- to index fields.

<a name="entity-models.indices.index"></a>
- **`INDEX`** - The distinct name of an index in Elasticsearch.

<a name="entity-models.indices.attribute-matcher"></a>
- **`ATTRIBUTE.MATCHER`** - The name of an `ATTRIBUTE` and `MATCHER` concatenated by a period.

<a name="entity-models.indices.field"></a>
- **`FIELD`** - The distinct name of a field in the index.


#### <a name="entity-models.resolvers">`"resolvers"`</a>

Resolvers are combinations of attributes that lead to a resolution. For example, you might decide to resolve entities
that share matching values for `"name"` and `"dob"` or `"name"` and `"phone"`. You can create a "resolver" for both
combinations of attributes. Then any documents whose values share either a matching `"name"` and `"dob"` or  `"name"`
and `"phone"` will resolve to the same entity.

Remember that attributes can have more than one matcher. This means that if *any* matcher of an attribute yields a
match for a given value, then the attribute will be considered a match regardless of the results of the other matchers.
So if you have an attribute called `name` with matchers called `keyword` and `phonetic`, then any resolver that uses
the `name` attribute is effectively saying that *either* `name.keyword` *or* `name.phonetic` are required to match.

<a name="entity-models.resolvers.resolver"></a>
- **`RESOLVER`** - The distinct name of the resolver. Each resolver represents one combination of attributes that lead
to resolution. The name cannot include periods The value of the resolver is an array of strings, each of which
represents the name of an `ATTRIBUTE`.

<a name="entity-models.resolvers.attribute"></a>
- **`ATTRIBUTE`** - The distinct name of an attribute. Some examples could include `"name"`, `"dob"`, `"phone"`, etc.
The name cannot include periods.


### <a name="entity-models.tips">Tips for designing entity models</a>

**1. Become familiar with your data.**

The real world has countless entity types and ways of resolving them. Start with what's relevant to your data. Look at
your data and understand the qualities of the fields and values within them. Are the values consistent? Are the values
created by end users with poor quality control measures? Are there duplicate values? Are there empty values? Do some
values appear more frequently than others? Which fields have high cardinality? Which fields are useful or useless for
identification purposes?

Knowing your data will help you determine what entity types you can resolve, which attributes constitute which entity
type, and what logic is needed to match the attributes and resolve the entities.

**2. Outline the attributes of your entity types.**

The first step to understanding an entity is to think about the attributes that describe it. Useful attributes will
include anything that can help identify an entity. For example, some common attributes to identify a person include
`name`, `address`, `dob`, `email`, `phone`, `ssn`, etc. Some attributes can also be represented in different ways. For
example, you might have an attribute for `address` and more specific attributes for `street`, `city`, `state`, `zip`,
`country`.

Start with the attributes that you know exist in your data. Don't worry about how to match an email address if you
don't have any email addresses in your data. Afterward, you can consider any additional attributes that you might see
in future data sets. You can always update your entity models later without having to reindex any data, so there's no
pressure to get it right the first time.

**3. Determine the matching logic for each attribute.**

You need to write at least one [matcher](#entity-models.attributes.matcher) for each attribute. A matcher is simply a
clause of a [`"bool"` query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html)
in Elasticsearch. Some attributes might have exact matches. Some attributes such as a `name` will tolerate
[fuzziness](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html) or target
[phonetic tokens](https://www.elastic.co/guide/en/elasticsearch/guide/current/phonetic-matching.html), while other
attributes such as an `email address` might not.

Below is an example of an attribute called `name` with two matchers called `text` and `phonetic`. You might use the
`name.text` matcher, which uses  the `"fuzziness"` field to allow for typos, on indexed name fields that used the
[standard analyzer](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-standard-analyzer.html).
You might use the `name.phonetic` matcher on indexed name fields that used a
[phonetic token filter](https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-phonetic-token-filter.html),
which is already a loose match that wouldn't benefit from the `"fuzziness"` field and might even generate more false
positives if you did use it.

*Example*

```javascript
{
  "attributes": {
    "name": {
      "text": {
        "match": {
          "{{ field }}": {
            "query": "{{ value }}",
            "fuzziness": 2
          }
        }
      },
      "phonetic": {
        "match": {
          "{{ field }}": {
            "query": "{{ value }}",
            "fuzziness": 0
          }
        }
      }
    }
  }
}
```

**4. Determine which combinations attributes lead to resolution.**
 
Usually you don't want to rely on a single attribute to resolve an entity. Imagine how many false positives you would
get if you tried to resolve a person by a name, like John Smith! Even attributes like Social Security Numbers (SSNs)
can be fraught with errors such as typos or bogus numbers, and there are valid reasons why a person might change an
SSN.

Instead, try to write resolvers that use combinations of attributes to avoid those "snowballs" of false positives.
Each combination represents a minimum amount of matching attributes that you would need to resolve an entity with
confidence. Below is an example that shows how you might combine the attributes `name`, `dob`, `street`, `city`,
`state`, `zip`, `email`, `phone` to resolve a `person` entity type.

*Example*

```javascript
{
  "resolvers": {
    "name_dob_city_state": [
      "name", "dob", "city", "state"
    ],
    "name_street_city_state": [
      "name", "street", "city", "state"
    ],
    "name_street_zip": [
      "name", "street", "zip"
    ],
    "name_email": [
      "name", "email"
    ],
    "name_phone": [
      "name", "phone"
    ],
    "email_phone": [
      "email", "phone"
    ]
  }
}
```

What combinations of attributes are right for you? That depends entirely on your data and your tolerance to errors.
You will need to experiment do determine what combinations of attributes yield satisfactory error rates on your
particular data sets.

**5. Use custom analyzers to index data in clever ways to improve accuracy.**
 
One of the goals of zentity is to prevent you from ever needing to reindex your data. But there are still cases
where you might want to do this. For example, you might have an indexed field called `name` that was indexed using
the [standard analyzer](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-standard-analyzer.html).
You can write a matcher that performs a basic match on this field, perhaps allowing for some fuzziness. But you
might want to have a [phonetic](https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-phonetic-token-filter.html) matcher, too. There are many ways to spell transliterated names, such as Muhammad: *Muhammed, Muhamad,
Muhamed, Muhamet, Mahamed, Mohamad, Mohamed, Mohammad, Mohammed*, etc. All of these spelling variations can be reduced
to the same phonetic value. But that value has to exist in the index if we want to use it for matching. If it doesn't
exist, you would need to update your index mapping to create a field that uses a [custom analyzer](https://www.elastic.co/guide/en/elasticsearch/guide/current/custom-analyzers.html)
using a phonetic tokenizer, and then [reindex](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html)
the data.


## <a name="rest-apis">REST APIs</a>

### <a name="resolution-api">Resolution API</a>

Runs an entity resolution job and returns the results.

The request accepts two endpoints:

```javascript
POST _zentity/resolution
POST _zentity/resolution/{entity_type}
```

**Example request:**

This example request resolves a `person` identified by a `name`, a `dob`, and two `phone` values, while limiting the
search to one index called `users_index` ane two resolvers called `name_dob` and `name_phone`.

```javascript
POST _zentity/resolution/person?pretty
{
  "attributes": {
    "name": "Alice Jones",
    "dob": "1984-01-01",
    "phone": [
      "555-123-4567",
      "555-987-6543"
    ]
  },
  "filter_indices": [
    "users_index"
  ],
  "filter_resolvers": [
    "name_dob",
    "name_phone"
  ]
}
```

**Example response:**

This example response took 64 milliseconds and returned 2 hits. The `_source` field contains the fields and values
as they exist in the document indexed in Elasticsearch. The `_attributes` field contains any values from the
`_source` field that can be mapped to the [`"attributes"`](#entity-models.attributes) field of the entity model.
The `_hop` field shows the level of recursion at which the document was fetched. Entities with many documents can
span many hops if they have highly varied attribute values.

```javascript
{
  "took": 64,
  "hits": {
    "total": 2,
    "hits": [
      {
        "_index": "users_index",
        "_type": "doc",
        "_id": "iaCn-mABDJZDR09hUNon",
        "_hop": 0,
        "_attributes": {
          "city": "Beverly Halls",
          "first_name": "Alice",
          "last_name": "Jones",
          "phone": "555 123 4567",
          "state": "CA",
          "street": "123 Main St",
          "zip": "90210-0000"
        },
        "_source": {
          "@version": "1",
          "city": "Beverly Halls",
          "fname": "Alice",
          "lname": "Jones",
          "phone": "555 987 6543",
          "state": "CA",
          "street": "123 Main St",
          "zip": "90210-0000"
        }
      },
      {
        "_index": "users_index",
        "_type": "doc",
        "_id": "iqCn-mABDJZDR09hUNoo",
        "_hop": 0,
        "_attributes": {
          "city": "Beverly Hills",
          "first_name": "Alice",
          "last_name": "Jones",
          "phone": "(555)-987-6543",
          "state": "CA",
          "street": "123 W Main Street",
          "zip": "90210"
        }
        "_source": {
          "@version": "1",
          "city": "Beverly Hills",
          "fname": "Alice",
          "lname": "Jones",
          "phone": "(555)-987-6543",
          "state": "CA",
          "street": "123 W Main Street",
          "zip": "90210"
        }
      }
    ]
  }
}
```

**URL query string parameters:**

|Param|Type|Default|Required|Description|
|-----|----|-------|--------|-----------|
|`_attributes`|Boolean|`true`|No|Return the `_attributes` field in each doc.|
|`_source`|Boolean|`true`|No|Return the `_source` field in each doc.|
|`hits`|Boolean|`true`|No|Return the `hits` field in the response.|
|`max_docs_per_query`|Integer|`1000`|No|Maximum number of docs per query result.|
|`max_hops`|Integer|`100`|No|Maximum level of recursion.|
|`pretty`|Boolean|`true`|No|Indents the JSON response data.|
|`profile`|Boolean|`false`|No|[Profile](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-profile.html) each query. Used for debugging.|
|`queries`|Boolean|`false`|No|Return the `queries` field in the response. Used for debugging.|

**Request body parameters:**

|Param|Type|Default|Required|Description|
|-----|----|-------|--------|-----------|
|`attributes`|Object| |Yes|The initial attribute values to search.|
|`entity_type`|String| |Depends|The entity type. Required if `model` is not specified.|
|`filter_indices`|Array| |No|The names of indices to limit the job to.|
|`filter_resolvers`|Array| |No|The names of resolvers to limit the job to.|
|`model`|Object| |Depends|The entity model. Required if `entity_type` is not specified.|

**Notes:**

- If you define an `entity_type`, zentity will use its model from the `.zentity-models` index.
- If you don't define an `entity_type`, then you must include a `model` object in the request body.
- You can define an `entity_type` in the request body or the URL, but not both.

**Tips:**

- If you only need to search a few indices, name them in the `filter_indices` parameter to prevent the job from
searching each index in the entity model at each hop.
- Beware if your data is ***transactional*** or has ***many duplicates***. You might need to lower the values of
`max_hops` and `max_docs_per_query` if your jobs are timing out.


### <a name="models-api">Models API</a>

#### Get all entity models

```javascript
GET _zentity/models
```

Returns all entity models from the `.zentity-models` index.


#### Get an entity model

```javascript
GET _zentity/models/{entity_type}
```

Returns the entity model for a given `entity_type` from the `.zentity-models` index.

**URL Params:**

|Param|Type|Default|Required|Description|
|-----|----|-------|--------|-----------|
|`entity_type`|String| |Yes|Entity type.|


#### Create an entity model

Creates an entity model for a given `entity_type` and puts it in the `.zentity-models` index.
Returns an error if an entity model already exists for that `entity_type`.

```javascript
POST _zentity/models/{entity_type}
{
  "attributes": {
    ATTRIBUTE: {
      MATCHER: QUERY_TEMPLATE,
      ...
    },
    ...
  },
  "indices": {
    INDEX: {
      ATTRIBUTE.MATCHER: FIELD,
      ...
    },
    ...
  },
  "resolvers": {
    RESOLVER: [
      ATTRIBUTE,
      ...
    ],
    ...
  }
}
```

**URL Params:**

|Param|Type|Default|Required|Description|
|-----|----|-------|--------|-----------|
|`entity_type`|String| |Yes|Entity type.|


#### Update an entity model

Updates an entity model for a given `entity_type`.
Creates the entity model if it does not already exist.

```javascript
PUT _zentity/models/{entity_type}
{
  "attributes": {
    ATTRIBUTE: {
      MATCHER: QUERY_TEMPLATE,
      ...
    },
    ...
  },
  "indices": {
    INDEX: {
      ATTRIBUTE.MATCHER: FIELD,
      ...
    },
    ...
  },
  "resolvers": {
    RESOLVER: [
      ATTRIBUTE,
      ...
    ],
    ...
  }
}
```

**URL Params:**

|Param|Type|Default|Required|Description|
|-----|----|-------|--------|-----------|
|`entity_type`|String| |Yes|Entity type.|


#### Delete an entity model

Deletes the entity model for a given `entity_type` from the `.zentity-models` index.

```javascript
DELETE _zentity/models/{entity_type}
```



**URL Params:**

|Param|Type|Default|Required|Description|
|-----|----|-------|--------|-----------|
|`entity_type`|String| |Yes|Entity type.|


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
