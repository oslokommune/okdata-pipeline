# converters.csv

## Task config

```
{
  "chunksize": number,
  "delimiter": string, # e.g. "tab", default is ","
  "schema": object
}
```

## Analysis

### 2019.11.29: Large file support
Processing large files with CSV to Parquet is limited by memory. Lambda will run
out of memory on about 8-900MB of CSV data (first discovered when Deichman sent
data to the pipeline).

Analysis on files from Deichman was done with the strategy to read X-lines in
chunks and save each transformation from CSV to Parquet to separate files, and
in the end run `fastparquet.writer.merge()` to calculate the metadata schema for
the files. After writing files, Glue was run on the output folder to create a
table that Athena could query.

#### Unzipped file
The result was as follows:

##### 1 - File: 1.08GB, 4434555 rows
* Spent: 141.43s to read, convert and write 46 chunks with 100000 lines per chunk
* Duration: 141534.66 ms	Billed Duration: 141600 ms	Memory Size: 1024 MB	Max Memory Used: 429 MB	Init Duration: 3283.37 ms

##### 2 - File: 1.08GB, 4434555 rows
* Spent: 141.82s to read, convert and write 10 chunks with 500000 lines per chunk
* Duration: 142014.96 ms	Billed Duration: 142100 ms	Memory Size: 1024 MB	Max Memory Used: 919 MB	Init Duration: 3378.46 ms

##### 3 - File: 1.08GB, 4434555 rows
* Spent: 105.17s to read, convert and write 6 chunks with 1000000 lines per chunk
* Duration: 105288.03 ms	Billed Duration: 105300 ms	Memory Size: 2048 MB	Max Memory Used: 1585 MB	Init Duration: 3273.37 ms

##### 4 - File: 1.08GB, 4434555 rows
* Spent: 112.87s to read, convert and write 4 chunks with 1500000 lines per chunk
* Duration: 113114.46 ms	Billed Duration: 113200 ms	Memory Size: 3008 MB	Max Memory Used: 2231 MB	Init Duration: 3280.80 ms

##### 5 - File: 4.31GB, 17738217 rows
* Spent: 430.209s to read, convert and write 19 chunks with 1000000 lines per chunk
* Duration: 430373.96 ms	Billed Duration: 430400 ms	Memory Size: 3008 MB	Max Memory Used: 1605 MB	Init Duration: 3191.32 ms
* Athena: 17738216 rows (+1 header row)

#### GZ file

##### File: 2.27GB gzipped & 7.54GB unzipped, 31041879 rows
* Spent: 711.86s to read, convert and write 33 chunks with 1000000 lines per chunk
* Duration: 711956.13 ms	Billed Duration: 712000 ms	Memory Size: 3008 MB	Max Memory Used: 1578 MB	Init Duration: 3182.83 ms
* Athena: Found 31041878 rows (+1 header row)

#### Conclusion
After running several file sizes and both gzipped and raw files the following
was found:

* 1.000.000 lines per chunk looks like a sweet-spot for duration and memory
  usage for a 1GB file
  * 1585MB Memory
  * 105 seconds to process 4million rows, both fewer and more rows per chunk
    resulted in longer processing time
* A file just under the 5GB PUT limit (our current limitation for files in the
  dataplatform)
  * about 7min execution time, well under 15min max time available for Lambda
  * time spent is about linear to file with 1/4 the size
* The 2.27GB gzipped file (7.54GB raw) scales almost linear in time spent
  compared to 1GB unzipped file
  * Memory usage is the same as 1GB unzipped with 1mill rows per chunk
* After running Glue & counting rows in Athena we see that Athena finds the
  correct amount of rows compared to raw input

#### Merging multiple files after split into 1 parq file
`fastparquet.write()` can take an `append=True` parameter, but this mode is not
supported when open_with is a s3fs pointer, and will end up with `raise
NotImplementedError("File mode not supported")`.

Setting `file_scheme=hive` does not make sense as it will try to open and write
to a read-only filesystem in Lambda environment.

#### Recommendations
* Use .gz for all CSV files
* Don't  upload more than 2.25GB gzipped or 4.5GB unzipped CSV
* Lambda
  * set timeout: 15 minutes
    * The gz file with 31 million rows was well within this
  * memory: 3008MB
    * Not 2048: to have room for big datasets per line (reading is done on
      row-count not on memory usage)
    * The example from Deichman (with 23 columns) used about 1500MB memory, but
      with (potentially more columns and content) the memory usage will be
      higher
* Lambda is not the best solution going forward if we are going to handle really
  large datasets
