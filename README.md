# Dictgensim

A simple python multiprocessing program to simulate overhead of parallel / distributed dictionary encoding.

## File producer and file input
Given a set of keys (with arg *--dict*), Dictgensim could generate input files with given keys in different distributions.

Uniform distribution and configurable zipf distribution are supported in file producer.

It could produce file with layout in both column-oriented and row-oriented ways.

## 1.x-pass dictionary encoding
Dictgensim provides 1-pass dictionary encoding simulation in distributed environment with configurable message latency (in norm distribution) and it also supports 0.x level of lookahead.

It could emulate dictionary generation and encoding on any *.csv* files

## Usage
    usage: dictsim.py [-h] [--file FILE] [--filelayout {column,table}]
                   [--workers WORKERS] [--colnum COLNUM]
                   [--protocol {leader,bully}] [--wordcount WORDCOUNT]
                   [--distribution {weighted,uniform,zipf}] [--zipfarg ZIPFARG]
                   [--batch BATCH] [--delay DELAY] [--lookahead LOOKAHEAD]

    Simulate a 1.x-pass dictionary encoding. File producer loads given file as
    word bank and produces intermediate files according to the given
    configuration(eg. word count, word distribution, file layout, etc). These
    intermediate files are set as input for dictionary encoding simulation.

    optional arguments:
      -h, --help            show this help message and exit
      --file FILE           The word bank file from which dictionary keys is
                            loaded, defaults to /usr/share/dict/words
      --filelayout {column,table}
                            File producer uses the given file as word bank to make
                            one column file or multiple column table file,
                            defaults to column
      --workers WORKERS     Number of workers for each experiment combination,
                            defaults to array={10,}
      --colnum COLNUM       Column number for target field to be encoded in
                            generated table file, defaults to 0
      --protocol {leader,bully}
                            The consensus protocol, defaults to leader
      --wordcount WORDCOUNT
                            Total number of words in all intermediate files for
                            each experiment combination, defaults to
                            array={40000,}
      --distribution {weighted,uniform,zipf}
                            The distribution (uniform/zipf) of words in input
                            files, defaults to zipf
      --zipfarg ZIPFARG     Parameter for zipf distribution when zipf is used,
                            defaults to 2.0
      --batch BATCH         Number of new keys in each new key proposal for each
                            experiment combination,defaults to array={16,}
      --delay DELAY         Latency(s) with norm distribution is added to each
                            message for each experiment combination, defaults to
                            array={0}
      --lookahead LOOKAHEAD
                            Portion of the file to look ahead for each experiment
                            combination, defaults to array={0.0, 0.1, ..., 1.0}


## Requirements
 * Python 3.0
 * numpy


## Protocols supported ##
* 2PC-Bully

    The basic idea is quite similar to 2PC protocol, except that there is a total order of the processes. If a coordinator receives a PREPARE message, it votes NO if and only if the sender is less than it. This prevents situations that arise where two coordinators both time out due to receiving each other’s NO vote.
    Value space is splited among different workers to avoid value conflicts.

* Leader

    An extra leader process is spawned through which all communication happens. Processes that want to commit a new key send it to the leader’s queue and the leader assigns a value to it and sends the pair to all processes. In this way, the leader’s local view is the de facto global view.

## Protocols to Add ##
Paxos, Raft, ...

If you want to contribute to Dictgensim simulator, you may refer to *Base_class.py*. You can find base class *Peer* used for building new protocol, and *Tester* for protocol test where pipe-based message system and lookahead mechanism are implemented.

### Contributing Authors ###
 Chunwei Liu

 Sotiri Komissopoulos

 Aaron Elmore
