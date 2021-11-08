import datetime
import sys
import os
import numpy
import pandas as pd


def get_output_path(path):
    file = open(path, "r")
    output = ""
    for line in file:
        if "Output_Path: " in line:
            line = line.replace("\n", "")
            line = line.replace("\r", "")
            output += line[20:]
    return output


def get_function_title(path):
    file = open(path, "r")
    call = ""
    for line in file:
        if "Function_Title: " in line:
            call += line[16:]
    return call


def get_map_function(path):
    file = open(path, "r")
    fun = ""
    for line in file:
        if "Function: " in line:
            fun += line[10:]
            for l in file:
                fun += l
    return fun


def get_seperator(path):
    file = open(path, "r")
    print(path)
    print(file)
    sep = ""
    for line in file:
        if "Seperator: " in line:
            sep = line[11:].strip()
    return sep


def get_csv_elements(path, seperator, schema):
    df = pd.read_csv(path, header=None, index_col=None, delimiter=seperator)
    return_list = []
    cols = []
    return_type_list = "["
    # Create return type list for map function later on
    for t in range(len(df.dtypes)):
        if df.dtypes[t] == float:
            print("FLOAT")
            return_type_list += "DataTypes.FIELD('" + schema[t] + "', DataTypes.FLOAT())"
        elif df.dtypes[t] == object:
            print("String")
            return_type_list += "DataTypes.FIELD('" + schema[t] + "', DataTypes.STRING())"
        elif df.dtypes[t] == int:
            print("Int")
            return_type_list += "DataTypes.FIELD('" + schema[t] + "', DataTypes.BIGINT())"
        elif df.dtypes[t] == bool:
            print("Bool")
            return_type_list += "DataTypes.FIELD('" + schema[t] + "', DataTypes.BOOLEAN())"
        elif df.dtypes[t] == datetime.datetime:
            print("date")
            return_type_list += "DataTypes.FIELD('" + schema[t] + "', DataTypes.DATE())"
        if t == (len(df.dtypes) - 1):
            return_type_list += "]"
        else:
            return_type_list += ", "

    # Go through the csv and create a tuple list with the correct types
    for i in range(len(df)):
        for j in range(len(schema)):
            if type(df[j][i]) == numpy.float64 or type(df[j][i]) == numpy.int64 or type(df[j][i]) == numpy.bool_:
                cols.append(df[j][i].item())
            else:
                cols.append(df[j][i])

        return_list.append(tuple(cols))
        cols = []

    return return_list, return_type_list


# Read the given schema
def get_schema(path, seperator):
    file = open(path, "r")
    lines = file.read()
    lines = lines.replace("\n", "")
    lines = lines.replace("\r", "")
    header = lines.split(seperator)
    return header


if __name__ == "__main__":

    # Sanity checks
    if len(sys.argv) is 1:
        print("please specify a data csv file")
        sys.exit()
    if len(sys.argv) is 2:
        print("please specify a schema as a .txt file")
        sys.exit()
    if len(sys.argv) is 3:
        print("please specify a config file")
        sys.exit()

    print(os.getcwd())

    # Get seperator from config file
    seperator = get_seperator(sys.argv[3])
    print(seperator)

    # Get the csv output path
    csv_output_path = get_output_path(sys.argv[3])

    # Get schema from schema file
    schema = get_schema(sys.argv[2], seperator)

    # Get elements as well as the column types from the csv file
    csv_elements, type_list = get_csv_elements(sys.argv[1], seperator, schema)

    # Get the function from the config file
    map_function = get_map_function(sys.argv[3])

    # Get the title of the function from the config file
    title = get_function_title(sys.argv[3])

    # Define the new python module to create
    new_module = f"""
from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.udf import udf


{map_function}

# Set batch mode setting for optimiued batch processing. This may be changed later for stream processing
settings = EnvironmentSettings.in_batch_mode()

# Create TableEnvironment which is the central object used in the Table/SQL API
t_env = TableEnvironment.create(settings)

# write all the data to one file
t_env.get_config().get_configuration().set_string("parallelism.default", "1")
table = t_env.from_elements({csv_elements}, {schema})
t_env.create_temporary_view('source_table', table)
tab = t_env.from_path("source_table")

map_function = udf({title},
                   result_type=DataTypes.ROW(
                       {type_list}),
                   func_type="pandas")

pd_table = tab.map(map_function).to_pandas()
pd_table.to_csv("{csv_output_path}" + "output.csv")
    """

    # Write the new module
    file = open("output.py", "w")
    file.write(new_module)
    file.close()
    while (True):
        pass
