# Remove items from one file using other file


## Before you begin

Make sure you have a [Maven](https://maven.apache.org/) 
## Source file structure

There are only two source files:

* [`src/main/java/com/zarr/Challenge.java`](src/main/java/com/zarr/Challenge.java): the application source file, containing the [`main` method](src/main/java/com/zarr/Challenge.java). The _main class_ is `com.zarr.Challenge`.
* [`src/test/java/com/zarr/ChallengeTest.java`](src/test/java/com/zarr/ChallengeTest.java): tests for the `Challenge.java` file.

### Apache Maven

To build a self-contained jar file:

```sh
# To run the tests
mvn test
# Build a self-contained jar.
mvn package

# Run the jar application.
# Mode is for naive with a hashset and a view and s is for sets
# deletion if you would like to preserve the data file, it will be save with and added .filtered
java -jar target/efx-challenge-1-jar-with-dependencies.jar --inputPath="path/to/dataKeys.csv" --filePath="path/to/data.csv" --mode="{n,s}" --deletion=false
```

# Thought Process

By reading the instructions my first thoughts were "This is a set membership problem"

With that in mind I used the two following approaches as a naive interpretation

- Store the keys in a fast access data structure `HashSet` then create a singleton view so all windows can access it in a `ParDo`
- Apache Beam has [Set](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/Sets.html) transformations to solve problems of set membership

## Problems with the approaches

As I was working on a very limited machine (12GB of ram) loading big files like 1 GB or even 500 MB resulted sometimes
in memory heap problems so, I wasn't able to test the full capacity of the implementations but for both implementations 
using files of 100MB resulted in similar CPU consumption, about 40% for the HashSet implementation and 37% for the
set transformations implementation.

Set transformations also waste CPU by sorting the data in the collection.

To test this I used [HyperFine](https://github.com/sharkdp/hyperfine) (exceptional tool) for warmups

I will expect CPU utilization would go up as file sizes increase because smaller datasets is not enough warrant good parallelism. 
The machine I'm using has 6 cores and the default `targetParallelism` of `Beam` is the number of cores or 3 if cores 
are less

## Why not open all files based on patterns

I did this and discarded it because it was less space efficient for the machine I was using, in an implementation with 
a new idea, is function not used in the code. 

This idea was to group all values
to match files in a KV<String, String> where the first String is a line in the file and the second is the filename,
with this approach I was planning on remove multiple repetitions of keys across all csv files

## Improvements
- Consider using a more efficient data structure like a bloom filter for lookUps,
  - the problem here is dealing with false positives, so if correctness is a constraint, it should be used alongside structure
- Implement the full idea of multiple with efficient key deletion alongside a more efficient data structure for lookUps

## Update
I was able to run the code in my main machine with bigger files size and the CPU utilization improved about 1GB for each
file

![1bg files](1GBFiles.png)

