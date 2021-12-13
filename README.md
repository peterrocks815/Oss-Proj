# Oss-proj
# How to use the Oss-Proj
# Step 1:
Create a "config" file. The already existing file in the input folder is there as an example.
The "config" has the following options:
- Use the "Seperator: " keyword followed by the seperator used in your .csv and schema.txt file (Both have to use the same seperator).
- Use the "Function_Title: " keyword followed by the declaration of your specified function.
- Use the "Function: " keyword followed by the code which will be executed by the programm. The code can be any arbitrary working python code, as long as the code has exactly one function
  that has the same declaration as the argument given in "Function_Title: ". This function must have exactly one argument, which is a pandas dataframe of the specified .csv file.
- If you want to use a SQL-query instead of a function to be executed on the given csv, use the keyword "SQL_Query: " followed by your sql query.
- If you are using a SQL-query then specify the name of the table after the keyword "Table_Name: ".
- If you want to execute any sql command (not just a SELECT query) please use the "SQL_Command: " keyword.
- ALL KEYWORDS HAVE TO BE SPECIFIED IN THE CONFIG FILE! If you dont want to use some of the keywords just leave them empty. (For example you specify a sql query you dont need to specify a function)
- If you want to use multiple SQL command you have to write the next command (after a ";") in a new line! 
- At the end of each option you have to put one empty line in order to signalize that the keyword is over!
- You can specify the following configurations for flink directly:
  - pipeline.auto-generate-uids: BOOLEAN
  - pipeline.auto-type-registration: BOOLEAN
  - pipeline.auto-watermark-interval: INT_DURATION_MS
  - pipeline.cached-files: LIST<String>
  - pipeline.classpaths: LIST<String>
  - pipeline.closure-cleaner-level: NONE or TOP_LEVEL or RECURSIVE
  - pipeline.default-kryo-serializers: List<String>
  - pipeline.force-avro: BOOLEAN
  - pipeline.force-kryo: BOOLEAN
  - pipeline.generic-types: BOOLEAN
  - pipeline.global-job-parameters: MAP
  - pipeline.jars: List<String>
  - pipeline.max-parallelism: Integer
  - pipeline.name: String
  - pipeline.object-reuse: BOOLEAN
  - pipeline.operator-chaining: BOOLEAN
  - pipeline.registered-kryo-types: List<String>
  - pipeline.registered-pojo-types: List<String>
  - pipeline.time-characteristic: "ProcessingTime" or "IngestionTime" or "EventTime"
# Step 2:
You need a .csv file with your data if you do not specify a table as a SQL command.
# Step 3:
You need a .txt file specifying the schema of your table. The column names should be seperated by the specified seperator.
# Step 4:
You need to install docker on your device.
# Step 5:
Add all additional requirements, which you used for your python function specified in the config file to the requirements.txt file.
# Step 6:
Run the command "docker-compose up".
# Step 7:
Open your browser and go to the shown ip address.
# Step 8:
Upload your config, your .csv and your .txt file by browsing for them and the clicking "Submit".
# Step 9:
When a .csv, a .txt and a config file have been found the server automatically creates the output.py file. You will be redirected to a new page. Click "Download!" to download a .zip folder containing the output.py file and your specified files.
# Step 10:
Call the file output.py with your installed python binary (for example "python3 output.py").
