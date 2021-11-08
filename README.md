# Oss-proj
# How to use the Oss-Proj
# Step 1:
Place the CSV-File with the data to be processed in the "input/" folder. It is important to name the file "data.csv", as the one which is already in the "input/" folder as an example.
#Step 2:
Place a "schema.txt" file with the schema of your data also inside the "input/" folder. As before the already existing "schema.txt" is an example on how it could look like. It is important that the file is called "schema.txt"!
#Step 3:
Create a "config" file and place it in the "input/" folder. The already existing file is also there as an example.
The "config" must satisfy the following requirements:
- There has to be exactly one "Seperator: " keyword followed by the seperator used in your .csv and schema.txt file (Both have to use the same seperator).
- There has to be exactly one "Function_Title: " keyword followed by the declaration of your specified function.
- There has to be exactly one "Function: " keyword followed by the code which will be executed by the programm. The code can be any arbitrary working python code, as long as the code has exactly one function
  that has the same declaration as the argument given in "Function_Title: ". This function must have exactly one argument, which is a pandas dataframe of the specified .csv file.
- If no "Output_Path: " is specified the final .csv output will be written as output.csv, otherwise the path will be "/your/specified/output/path/output.csv"
# Step 4:
You need to install docker on your device.
# Step 5:
Add all additional requirements, which you used for your python function specified in the config file to the requirements.txt file.
# Step 6:
Run the command "docker build -t oss-proj .".
# Step 7:
Run the command "docker run oss-proj".
# Step 8:
Once you see the output "FINISHED" on your command line, run the command "docker cp {container_name}:output.py .".
# Step 9:
Now run the command "docker stop {container_name}".
# Step 10:
Call the file output.py with your installed python binary (for example "python3 output.py").
# Step 11:
The final .csv file will be in the specified output path or simply at "./output.csv" if no path was specified.
