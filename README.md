# Oss-proj
# How to use the Oss-Proj
# Step 1:
Place the CSV-File with the data to be processed in the "input/" folder. It is important to name the file "data.csv", as the one which is already in the "input/" folder as an example. If you want to create a Table as a sql command create an empty data.csv and put it in the "input/" folder.
# Step 2:
Place a "schema.txt" file with the schema of your data also inside the "input/" folder. As before the already existing "schema.txt" is an example on how it could look like. It is important that the file is called "schema.txt"! Again if you want to create a Tabke as a sql command just leave the schema.txt empty.
# Step 3:
Create a "config" file and place it in the "input/" folder. The already existing file is also there as an example.
The "config" has the following options:
- Use the "Seperator: " keyword followed by the seperator used in your .csv and schema.txt file (Both have to use the same seperator).
- Use the "Function_Title: " keyword followed by the declaration of your specified function.
- Use the "Function: " keyword followed by the code which will be executed by the programm. The code can be any arbitrary working python code, as long as the code has exactly one function
  that has the same declaration as the argument given in "Function_Title: ". This function must have exactly one argument, which is a pandas dataframe of the specified .csv file.
- If you want to use a SQL-query instead of a function to be executed on the given csv, use the keyword "SQL_Query: " followed by your sql query.
- If you are using a SQL-query then specify the name of the table after the keyword "Table_Name: ".
- If you want to execute any sql command (not just a SELECT query) please use the "SQL_Command: " keyword.
- If no "Output_Path: " is specified the final .csv output will be written as output.csv, otherwise the path will be "/your/specified/output/path/output.csv"
- ALL KEYWORDS HAVE TO BE SPECIFIED IN THE CONFIG FILE! If you dont want to use some of the keywords just leave them empty. (For example you specify a sql query you dont need to specify a function)
- If you want to use multiple SQL command you have to write the next command (after a ";") in a new line! 
- At the end of each option you have to put one empty line in order to signalize that the keyword is over!
# Step 4:
You need to install docker on your device.
# Step 5:
Add all additional requirements, which you used for your python function specified in the config file to the requirements.txt file.
# Step 6:
Run the command "docker build -t oss-proj .".
# Step 7:
Run the command "docker run oss-proj".
# Step 8:
Run the command "docker cp {container_name}:output.py ." in another terminal. (You can find the container_name in the column CONTAINER ID when using the command docker ps)
# Step 9:
Now run the command "docker stop {container_name}".
# Step 10:
Call the file output.py with your installed python binary (for example "python3 output.py").
# Step 11:
The final .csv file will be in the specified output path or simply at "./output.csv" if no path was specified.
