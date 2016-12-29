
# Linden

![Linden Logo](docs/images/linden-logo-120px.png)

Linden is a distributed and real-time search system built on top of Lucene. Linden is widely used in Xiaomi. Linden provides a SQL-like query language interface named BQL(Browsing Query Language). BQL is simple and straightforward. Linden provides a very simple way to tune search result rankings. You can pass a piece of JAVA scoring code or even a scoring plugin class name in your query. Linden also supports customizing scoring logic from low index level via linden flexible query, you can get each query term match information(position, frequency and score), so that you can make a very intuitive scoring logic. This is very convenient for beginners.

## Get Started 

*   git clone https://github.com/XiaoMi/linden.git
*   cd to root directory of linden source code
*  `$ mvn clean package -DskipTests`
*  `$ sh ./bin/start-zk-server.sh &` or run `> .\bin\start-zk-server.cmd`  in windows OS
*  `$ sh ./bin/start-linden-server.sh demo/cars/conf/`  or  run `> .\bin\start-linden-server.cmd  demo/cars/conf/` in windows OS
*   Play the demo at [http://localhost:10000](http://localhost:10000)

## Linden Overview

Please see [Linden Overview Document](docs/LindenOverview.md).

## Linden Schema

Linden has a static schema file to specify document field property(name, type, index, store, etc.). Linden also supports dynamic field schema. See more in [Linden Schema Document](docs/LindenSchema.md).

## BQL Introduction

Please see [BQL Document](docs/BQL.md).

## Linden Flexible Query

Please see [Linden Flexible Query Document](docs/LindenFlexibleQuery.md).

## Linden Work Mode

Linden has 3 work modes for different scenarios: simple mode, hot-swap mode and multi-core mode. See more [Linden Work Mode Document](docs/LindenWorkMode.md).

## Linden Plugin

Linden provides user the ability to rewrite some linden component in plugin mode, for example Analyzer, Merge Policy, Metric Manager, Index Warmer and Similarity. See more in [Linden Plugin Document](docs/LindenPlugin.md).

## Linden Client

Linden provides a java client, see detail in [Linden Client Document](docs/LindenClient.md).

## Linden HTTP API

Linden also provides RESTful HTTP API, see detail in [Linden HTTP API Document](docs/LindenHTTPAPI.md).

## Linden Performance

We have done a simple perf test, see detail in [Linden Performance Test Document](docs/LindenPerformanceTest.md).

## Linden Configuration

Please see [Linden Properties Document](docs/LindenProperties.md).

## Developers

**Yonghui Zhao** ([@yozhao](https://github.com/yozhao)),
**Yang Li** ([@philolee](https://github.com/philolee)),
**Aibao Luo** ([@roaporl](https://github.com/roaporl)),
**Xing Wang** ([@intergret](https://github.com/intergret)),
**Bin Qin** ([@whuqin](https://github.com/whuqin)),
**Hucheng Huang** ([@kenghuang](https://github.com/kenghuang))