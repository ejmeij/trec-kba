## Hadoop code for TREC KBA   

This project contains some Hadoop code for working with the [TREC Knowledge Base Acceleration (TREC KBA)](http://trec-kba.org/) dataset. In particular, it provides classes to read/write topic files, read/write run files, and expose the documents in the Thrift files as Hadoop-readable objects (```ThriftFileInputFormat```).      

## Installing

### Dependencies 

The project requires some external jars, see below. Please download them and place them in the lib/ folder.

*   [CodeModel 2.4.1](http://repo1.maven.org/maven2/com/sun/codemodel/codemodel/2.4.1/codemodel-2.4.1.jar)
*   [Commons Lang 2.5](http://repo1.maven.org/maven2/commons-lang/commons-lang/2.5/commons-lang-2.5.jar)
*   [Commons Logging 1.1.1](http://central.maven.org/maven2/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar)
*   [Jackson Core 2.0.0](http://repo2.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.0.0/jackson-core-2.0.0.jar)
*   [Jackson Databind 2.0.0](http://repo2.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.0.0/jackson-databind-2.0.0.jar)
*   [Jackson Annotations 2.0.0](http://repo2.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.0.0/jackson-annotations-2.0.0.jar)
*   [hadoop-core-0.20.2-cdh3u3.jar](https://repository.cloudera.com/content/repositories/releases/org/apache/hadoop/hadoop-core/0.20.2-cdh3u3/hadoop-core-0.20.2-cdh3u3.jar)
*   [libthrift-0.8.0.jar](http://repo2.maven.org/maven2/org/apache/thrift/libthrift/0.8.0/libthrift-0.8.0.jar)
*   [log4j 1.2.14](http://repo2.maven.org/maven2/log4j/log4j/1.2.14/log4j-1.2.14.jar)  

### Building

Then, to build, type

```ant -f trec-kba.xml```

Eclipse project files are included, so you can also directly check out/build the code there.

## Running

The project comes with two example Hadoop applications; a simple genre counter and a toy KBA system. Both assume that the KBA files have been downloaded, un-gpg'ed, and un-xz'ed. in the official folder structure. Below, I'm assuming the root folder is ```kba/kba-stream-corpus-2012-cleansed-only-out```.    

To actually fetch the [data](http://trec-kba.csail.mit.edu/kba-stream-corpus-2012/) (if you haven't already done so), you can write a simple shell script and use Hadoop streaming to fetch, un-gpg, and un-xz the data.   

### Genre counter

This application merely counts the the different genres (web, social, news) in the KBA data. To run, use 

    hadoop jar trec-kba.jar ilps.hadoop.bin CountGenres \
        -i kba/kba-stream-corpus-2012-cleansed-only-out/*/* \
        -o kba/kba-stream-corpus-2012-cleansed-only-genre-counts

### Toy KBA system

This application is inspired by the [Python toy KBA system](http://trec-kba.org/kba-ccr-2012.shtml) and provides similar functionality. To run, use
    
    hadoop jar trec-kba.jar ilps.hadoop.bin.ToyKbaSystem \
        -i kba/tiny-kba-stream-corpus/*/* \
        -o kba/tiny-kba-stream-corpus-out \
        -q kba/filter-topics.sample-trec-kba-targets-2012.json \
        -r toy_02 -t UvA -d "My first run." \
        > toy_kba_system.run_1.json

Note that the ```tiny-kba-stream-corpus``` can be found in the [official toy KBA system](http://trec-kba.org/toy-kba-system.tar.gz).

Type ```hadoop jar trec-kba.jar ilps.hadoop.bin.ToyKbaSystem --help``` for all possible options. 

## Issues

Have a bug? Please create an issue here on GitHub!

## License

Copyright 2012 [Edgar Meij](http://edgar.meij.pro).

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0