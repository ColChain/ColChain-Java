# ColChain: Collaborative Linked Data Networks
ColChain is a new unstructured Peer-to-Peer architecture for sharing and querying Semantic data. ColChain builds on top of [PIQNIC](https://github.com/Chraebe/PIQNIC) and applies community-based chains/ledgers of updates to Knowledge Graphs. 
* [Abstract](#abstract)
* [Requirements](#requirements)
* [Installation](#installation)
	* [Build](#build)
	* [Deploy](#deploy)
	* [Setup](#setup)
* [Status](#status)
* [Running Experiments](#running-experiments)
# Abstract
The vision of the Semantic Web is to make the Web of Data available to a broad range of applications and users. Yet, in its current form it relies totally on the data providers to keep the data available and up-to-date; this, however, imposes a large burden on the data providers meaning that not all of them can fulfill these expectations for an extended period of time. Hence, in recent years we have seen a movement towards more decentralized architectures making use of technology such as Peer-to-Peer and relying on replication of the data. Although this helps with keeping the data available, to the best of our knowledge, no such system exists that lets users keep datasets up-to-date or query older versions. Therefore, in this paper we propose ColChain (Collaborative knowledge Chains), a novel decentralized architecture that lowers the burden on data providers and enables users to propose updates to faulty or outdated Linked Open Data. In addition, participating nodes can trace-back updates to their origin and process queries over previous versions of the published datasets. Our extensive experiments show that ColChain provides a solution that enables updates and versioning while achieving query processing performance comparable to state of the art Peer-to-Peer systems.
# Requirements
* Java 8 or newer
* Maven
* Application server such as [Jetty](https://www.eclipse.org/jetty/) or [Tomcat](http://tomcat.apache.org/)
# Installation
### Build
To install and run a ColChain node, build the project using Maven:
```
mvn install
```
Or to create a runnable Jar with dependencies:
```
mvn compile assembly:single
```
### Deploy
To run a ColChain node, you must create a Config file. Here is an example config.json file:
```json
{
  "title": "My ColChain Client",
  "datasourcetypes": {
    "HdtDatasource": "org.linkeddatafragments.datasource.hdt.HdtDataSourceType"
  },

  "datastore":"datastore/",
  "address":"http://colchain.org:8080",
  "prefixes": {
    "rdf":         "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs":        "http://www.w3.org/2000/01/rdf-schema#",
    "xsd":         "http://www.w3.org/2001/XMLSchema#",
    "dc":          "http://purl.org/dc/terms/",
    "foaf":        "http://xmlns.com/foaf/0.1/",
    "dbpedia":     "http://dbpedia.org/resource/",
    "dbpedia-owl": "http://dbpedia.org/ontology/",
    "dbpprop":     "http://dbpedia.org/property/",
    "hydra":       "http://www.w3.org/ns/hydra/core#",
    "void":        "http://rdfs.org/ns/void#"
  }
}
```
Once ColChain has been built, it can be deployed as standalone or in an application server.
To run a ColChain node standalone, use the following command:
```
java -jar colchain.jar config.json
```
To deploy in an application server, use the WAR file. Create an `config.json` configuration file with the data sources (analogous to the example file) and add the following init parameter to `web.xml`. If no parameter is set, it looks for a default `config-example.json` in the folder of the deployed WAR file.
```xml
<init-param>
  <param-name>configFile</param-name>
  <param-value>path/to/config/file</param-value>
</init-param>
```
### Setup
Once the node is started, go to the web interface by opening a Web browser and navigating to the URL, e.g., `http://colchain.org:8080/`. Enter the path to the config file in the Web interface (1st form from the top), e.g., `/path/to/config.json` and hit `Initiate`.
For experimental setup of a node, refer to [Running Experiments](#running-experiments).
# Status
ColChain is currently implemented as a prototype used for experiments in the ColChain paper. We are working on creating making the following features available in ColChain:
* Different RDF compression techniques
* User-chosen indexing schema
* More fragmentation functions
* More consensus protocols
* Easier to use user interface

Stay tuned for more such features!
# Running Experiments
### Creating a Setup
To create the setup directories, navigate in the Web browser to the URL, e.g., `http://colchain.org:8080/`. In the Web interface, enter the following (3rd form from the top):
* config file, e.g., `/path/to/config.json`
* data directory, e.g., `/path/to/data/`
* number of nodes in total, e.g., `128`
* replications per fragment (number of participants in each community), e.g., `10`

This will create the `setup` directory where all the needed files are located.
### Running a Setup
To setup a node for experiments, go to the Web interface by navigating to the correct URL, e.g., `http://colchain.org:8080/`. Enter the following (4th form from the top):
* config file, e.g., `/path/to/config.json`
* data directory, e.g., `/path/to/data/`
* setup directory, e.g., `/path/to/setup/` (see above)
* node ID, e.g., `0`
* number of nodes in total, e.g., `128`
* chain lengths, e.g., `100`

### Running Experiments
Now that the node is setup, use the Web interface to start the experiments.

