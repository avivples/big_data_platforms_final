# Big Data Platforms
## Final Project - Small files and MapReduce

### How to run the code:
Run the following command: `python main.py {num_of_files_to_create} {min_num_of_rows} {max_num_of_rows}`

A jupyter notebook has been added to the repository as a report of the code. The current checkpoint
of the notebook was run on Google Colab.

All files created are placed in the directory `output`. The code creates csv files, merges them,
then runs MapReduce on the large and merged files, and afterwards on the small files.

<br>

We ran an experiment on 500 CSV files, with number of rows ranging from 10 to 10,000.
We ran it once on Google Colab and another time locally using the code from this repository.
Both runs are timed, and the results clearly show that our merging method reduces the MapReduce runtime.

**Terminal local Run**:

The MapReduce took 108.875 seconds on the merged and large files

The MapReduce took 145.676 seconds on the small files

**Google Colab Run**:

The MapReduce took 261.533 seconds on the merged and large files

The MapReduce took 301.918 seconds on the small files

<br>

Finally, all temporary files in `output/mapreducetemp` and the SQLite file are deleted.

## Project Structure

### Creation of Random Small Files

We work locally as this is a prototype.

We create 500 different CSV files in this format:  `myCSV[Number].csv`, where each file contains a random number of records, $10 - 10,000$ (due to limited computing resources)

The schema is `(‘firstname’,’secondname’,city’)`  

Values randomly chosen from the lists: 
- `firstname` : `[John, Dana, Scott, Marc, Steven, Michael, Albert, Johanna]`  
- `city` : `[New York, Haifa, München, London, Palo Alto,  Tel Aviv, Kiel, Hamburg]`  
- `secondname`: any value

<br>

### Merge Small Files

Considering this is a prototype we do the merging as follows:

1. We segregate the large files from the small files
2. Sort the small files into the merging queue
3. Small files are put into the queue with the maximum merge limit
4. Files are placed into the mergin queue until the queue size becomes equal to the merge criteria
5. Finally we merge the files in each queue into one

<br>
This method only covers a prototype solution for CSV files

<br>

### MapReduceEngine

**Python class** `MapReduceEngine` with method:

`def execute(input_data, map_function, reduce_function)`:

- `input_data`: is an array of elements
- `map_function`: is a pointer to the Python function that returns a list where each entry of the form (key,value) 
- `reduce_function`: is pointer to the Python function that returns a list where each entry of the form (key,value)

<br>

`execute(...)` function:
<br>

1. For each key  from the  input_data, start a new Python thread that executes map_function(key) 

2. Each thread will store results of the map_function into mapreducetemp/part-tmp-X.csv where X is a unique number per each thread.

3. Keep the list of all threads and check whether they are completed.

4. Once all threads completed, load content of all CSV files into the temp_results table in SQLite.

    Remark: Easiest way to loop over all CSV files and load them into Pandas first, then load into SQLite  
    `data = pd.read_csv(path to csv)`  
    `data.to_sql(‘temp_results’,sql_conn, if_exists=’append’,index=False)`

5. **SQL statement** that generates a sorted list by key of the form `(key, value)` where value is concatenation of ALL values in the value column that match specific key. For example, if table has records
<table>
    <tbody>
            <tr>
                <td style="text-align:center">John</td>
                <td style="text-align:center">myCSV1.csv</td>
            </tr>
            <tr>
                <td style="text-align:center">Dana</td>
                <td style="text-align:center">myCSV5.csv</td>
            </tr>
            <tr>
                <td style="text-align:center">John</td>
                <td style="text-align:center">myCSV7.csv</td>
            </tr>
    </tbody>
</table>

    Then SQL statement will return `(‘John’,’myCSV1.csv, myCSV7.csv’)`
<br>

6. **Start a new thread** for each value from the generated list in the previous step, to execute `reduce_function(key,value)` 
    
7. Each thread will store results of reduce_function into `mapreducefinal/part-X-final.csv` file  

8. Keep list of all threads and check whether they are completed  

9. Once all threads completed, print on the screen `MapReduce Completed` otherwise print `MapReduce Failed`

### MapReduce Functions

`inverted_map(document_name)`:

Reads the CSV document from the local disc and return a list that contains entries of the form (key_value, document name).

For example, if myCSV4.csv document has values like:  
`{‘firstname’:’John’,‘secondname’:’Rambo’,‘city’:’Palo Alto’}`

Then `inverted_map(‘myCSV4.csv’)` function will return a list:  
`[(‘firstname_John’,’ myCSV4.csv’),(‘secondname_Rambo’,’ myCSV4.csv’), (‘city_Palo Alto’,’ myCSV4.csv’)]`

<br>
Reduce function:

`inverted_reduce(value, documents)`, where the field “documents” contains a list of all CSV documents per given value.   
This list might have duplicates.   
Reduce function will return new list without duplicates.

For example,  
calling the function `inverted_reduce(‘firstname_Albert’,’myCSV2.csv, myCSV5.csv,myCSV2.csv’)`   
will return a list `[‘firstname_Albert’,’myCSV2.csv, myCSV5.csv,myCSV2.csv’]`
