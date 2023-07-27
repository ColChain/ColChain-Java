
# ColChain: Collaborative Linked Data Networks
[![Docker Stars](https://img.shields.io/docker/stars/caebel/colchain.svg)](https://hub.docker.com/r/caebel/colchain/) [![Docker Stars](https://img.shields.io/docker/pulls/caebel/colchain.svg)](https://hub.docker.com/r/caebel/colchain/)

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
### Research paper
One of the major obstacles that currently prevents the Semantic Web from exploiting its full potential is that the data it provides access to is sometimes not available or outdated. The reason is rooted deep within its architecture that relies on data providers to keep the data available, queryable, and up-to-date at all times – an expectation that many data providers in reality cannot live up to for an extended (or infinite) period of time. Hence, decentralized architectures have recently been proposed that use replication to keep the data available in case the data provider fails. Although this increases availability, it does not help keeping the data up-to-date or allow users to query and access previous versions of a dataset. In this paper, we therefore propose ColChain (COLlaborative knowledge CHAINs), a novel decentralized architecture based on blockchains that not only lowers the burden for the data providers but at the same time also allows users to propose updates to faulty or outdated data, trace updates back to their origin, and query older versions of the data. Our extensive experiments show that ColChain reaches these goals while achieving query processing performance comparable to the state of the art.
### Demo paper
The current architecture of the Semantic Web fully relies on the individual data providers to maintain access to their data and to keep their data up to date. While this may seem like a practical and straightforward solution, it often results in the data being unavailable or outdated. In this paper, we present a fully functioning client along with a user-friendly interface for ColChain, a system that increases availability of knowledge graphs and enables users to update the data in a community-driven way while still allowing them to query old versions.
# Requirements
* ***Docker*** 20 or higher

*Or the following:*

* Java 8 or newer
* Maven
* Application server such as [Jetty](https://www.eclipse.org/jetty/) or [Tomcat](http://tomcat.apache.org/)
# Installation
To install and run ColChain, either use the Docker image available on Docker Hub [on this link](https://hub.docker.com/r/caebel/colchain), or follow the guide below.
## Install with Docker
To install from Docker, simply use the following command:
```
docker run -d -p <port>:8080 caebel/colchain
```
where `<port>` is the port you want your ColChain instance to be mapped to.

## Install without Docker
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
Now that the node is setup, use the Web interface to start the experiments (under the item "Experiments") by filling out the form with the relevant fields for the relevant experiments.

