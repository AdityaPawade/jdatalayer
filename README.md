# Java Data Layer  [![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT) ![Maven Central](https://img.shields.io/maven-central/v/com.adtsw.jdatalayer/rocksdb?color=blue&label=Version) ![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/com.adtsw.jdatalayer/rocksdb?label=Snapshot&server=https%3A%2F%2Foss.sonatype.org%2F)


***Connectors for common java projects***

---

### Features

* [x] Framework for easily connecting services with connectors 
* [x] Connectors for MapDB
* [x] Connectors for RocksDB

### Maven configuration

JDataLayer is available on [Maven Central](http://search.maven.org/#search). You just have to add the following dependency in your `pom.xml` file.

For RocksDB

```xml
<dependency>
  <groupId>com.adtsw.jdatalayer</groupId>
  <artifactId>rocksdb</artifactId>
  <version>1.0.22</version>
</dependency>
```
For MapDB

```xml
<dependency>
  <groupId>com.adtsw.jdatalayer</groupId>
  <artifactId>mapdb</artifactId>
  <version>1.0.22</version>
</dependency>
```

For ***snapshots***, add the following repository to your `pom.xml` file.
```xml
<repository>
    <id>sonatype snapshots</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
</repository>
```
The ***snapshot version*** has not been released yet.
```xml
<dependency>
  <groupId>com.adtsw.jdatalayer</groupId>
  <artifactId>rocksdb</artifactId>
  <version>TBD</version>
</dependency>
```
