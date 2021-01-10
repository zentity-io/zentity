# Contributing to zentity


## Feature requests and bug reports

Open an issue on the [issue list](https://github.com/zentity-io/zentity/issues).


## Development

### Setting up the development environment

zentity was developed in IntelliJ IDEA and uses Maven to manage dependencies, tests, and builds.

**Prepare your environment:**

1. Install [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
2. Install [IntelliJ IDEA](https://www.jetbrains.com/idea/download/)
3. Install [JDK 11](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)

**Create the project on IntelliJ IDEA:**

1. File > New > Project from Version Control
    - Version Control: Git
    - URL: https://www.github.com/zentity-io/zentity
    - Directory: Your local repository directory

**Configure the project to use JDK 11:**

1. Navigate to File > Project Structure
2. Navigate to Project Settings > Project Settings
3. Set the Project SDK to the home path of JDK 11

**Configure the Maven Run Configurations:**

1. Navigate to Run > Edit Configurations
2. Navigate to Maven
3. Create configurations with the following values for "Command line":
    - `clean install` - Runs all tests and builds zentity locally.
    - `clean install -e -X -Dtests.security.manager=false` - Runs all tests and builds zentity locally with extra debugging details.
    - `test -DskipIntegTests=true` - Runs unit tests locally.
4. Run these frequently to ensure that your tests continue to pass as you modify the code.

**Known issues:**

- When integration tests fail, the spawned Elasticsearch server will continue to run. You must find the process PID running on port 9400 and terminate it before you can run integration tests again.


### Important files

- **.travis.yml** - Build matrix for travis-ci.org
- **pom.xml** - Maven configuration file that defines project settings, dependency versions, build behavior, and high level test behavior.
- **src/main/resources/plugin-descriptor.properties** - File required by all Elasticsearch plugins ([source](https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-authors.html#_plugin_descriptor_file)).
- **src/test/ant/integration-tests.xml** - Ant script that controls the integration tests.
- **src/test/java** - Code for unit tests and integration tests. Unit tests are suffixed with `Test`. Integration test classes are suffixed with `IT`.
- **src/test/resources** - Data, entity models, and configurations used by tests.


### Software design choices

- **Outputs must be deterministic.** Use `TreeMap` and `TreeSet` instead of `HashMap` and `HashSet` for properties that will be serialized to JSON. This ensures that identical inputs produce identical outputs down to the last byte.
- **Input and outputs should mimic the experience of Elasticsearch.** This will make it easier for Elasticsearch users to work with the zentity. Here are some examples:
    - zentity and Elasticsearch both display results under `hits.hits`.
    - zentity and Elasticsearch use the `pretty` URL parameter to format JSON.


### Code formatting conventions

- Line separator: LF (`\n`)
- File encoding: UTF-8
- Indentation: 4 spaces
- Automatically reformat code with your IDE before committing. zentity uses the default reformatting configuration of IntelliJ IDEA.
- Break up large chunks of code into smaller chunks, and preface each code chunk with a brief comment that explains its purpose.


### Submitting contributions

1. Create a branch.
2. Develop your changes.
3. Rebase your changes with the master branch.
4. Test your changes.
5. Submit a [pull request](https://github.com/zentity-io/zentity/pulls). If your contribution addresses a feature or bug from the
[issues list](https://github.com/zentity-io/zentity/issues), please link your pull request to the issue.


## Contacting the author

zentity is maintained by [davemoore-](https://github.com/davemoore-) who can help you with anything you need regarding this project.
