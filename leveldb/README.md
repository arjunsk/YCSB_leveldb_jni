<!--
Copyright (c) 2012 - 2018 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on LevelDB running locally (within the same JVM).
NOTE: LevelDB is an embedded database and so articles like [How to run in parallel](https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload-in-Parallel) are not applicable here.

### 1. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone https://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:leveldb-binding -am clean package

### 2. Run YCSB

Now you are ready to run! First, load the data:

    ./bin/ycsb load leveldb -s -P workloads/workloada -p leveldb.dir=/tmp/ycsb-leveldb-data

Then, run the workload:

    ./bin/ycsb run leveldb -s -P workloads/workloada -p leveldb.dir=/tmp/ycsb-leveldb-data

## LevelDB Configuration Parameters

* ```leveldb.dir``` - (required) A path to a folder to hold the LevelDB data files.
    * EX. ```/tmp/ycsb-leveldb-data```