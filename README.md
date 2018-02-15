# etlpro

These are the programs I discussed in my talk **ETL for Pros – Getting Data Into MongoDB The Right Way** at MongoDB World 2016.  They show various ways to get from a relational table structure to MongoDB documents.

The script [create_data.py](create_data.py) sets up the source tables in a local MySQL database.

There are three approaches to get the data into MongoDB. The first three are bad for various reasons and to varying degrees. The final one, which I call the **co-iteration pattern**, attempts to overcome the shortcomings of the earlier ones.  For each solution, there's a simple variant that writes data directly to MongoDB, document by document, and a second variant that batches MongoDB operations in groups of 1,000 each.

Here's a full list:

* The first solution ([etl_1.py](etl_1.py), [etl_1_batch.py](etl_1_batch.py)) reads data from the source system in **nested queries**, leading to 2n+1 individual queries.
* The second solution ([etl_2.py](etl_2.py), [etl_2_batch.py](etl_2_batch.py)) only runs three queries against the source system (each getting one full table), and **builds the documents in the target database**, using individual MongoDB updates.
* The third solution ([etl_3.py](etl_3.py), [etl_3_batch.py](etl_3_batch.py)) starts by **reading the lookup tables completely into the application's memory**, and then assembles the output documents in a loop over the main source table.
* The final solution ([etl_co.py](etl_co.py), [etl_co_batch.py](etl_co_batch.py)) introduces the **co-iteration pattern**, which opens all source tables at once, performs a single pass over all of them, and assembles the output documents in the process.

For an actual tool that does all this, and more, in a more generic manner, see John Page's [https://github.com/johnlpage/MongoSyphon](MongoSyphon).
