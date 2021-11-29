import datetime
import sys
import numpy
import pandas as pd
from shutil import copyfile, make_archive


def get_sql_command(path):
    file = open(path, "r")
    command = "t_env.execute_sql('''"
    new_command = False
    for line in file:
        if "SQL_Command: " in line:
            command += line[13:]
            for l in file:
                if l is "\n" or l is "\r":
                    if command[-1] == "\n" or command[-1] == "\r":
                        command = command[: -1]
                    return command
                if ";" in l:
                    l = l.replace(";", " ''')")
                    if new_command:
                        command += "t_env.execute_sql('''"
                        command += l
                    else:
                        command += l
                    new_command = True
                    continue
                if new_command:
                    command += "t_env.execute_sql('''"
                    command += l
                    new_command = False
                else:
                    command += l

    if command[-1] == "\n" or command[-1] == "\r":
        command = command[: -1]
    return command


def get_sql_query(path):
    file = open(path, "r")
    query = ""
    for line in file:
        if "SQL_Query: " in line:
            query += line[10:]
            for l in file:
                if ";" in l:
                    l = l.replace(";", "")
                if l is "\n" or l is "\r":
                    return query
                query += l
    return query


def get_table_name(path):
    file = open(path, "r")
    name = ""
    for line in file:
        if "Table_Name: " in line:
            line = line.replace("\n", "")
            line = line.replace("\r", "")
            name += line[12:]
    return name


def get_output_path(path):
    file = open(path, "r")
    output = ""
    for line in file:
        if "Output_Path: " in line:
            line = line.replace("\n", "")
            line = line.replace("\r", "")
            output += line[13:]
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


def create_output(data_path=None, schema_path=None, config_path=None):
    # Sanity checks
    if data_path is None:
        print("please specify a data csv file")
        sys.exit()
    if schema_path is None:
        print("please specify a schema as a .txt file")
        sys.exit()
    if config_path is None:
        print("please specify a config file")
        sys.exit()

    # Get seperator from config file
    seperator = get_seperator(config_path)

    # Get the csv output path
    csv_output_path = get_output_path(config_path)

    # Get schema from schema file
    schema = get_schema(schema_path, seperator)

    # Get elements as well as the column types from the csv file
    csv_elements, type_list = get_csv_elements(data_path, seperator, schema)

    # Get the function from the config file
    map_function = get_map_function(config_path)

    # Get the title of the function from the config file
    title = get_function_title(config_path)

    # Get the title of the function from the config file
    table_name = get_table_name(config_path)
    if table_name is "":
        table_name = "source_table"

    # Get the title of the function from the config file
    sql_query = get_sql_query(config_path)

    sql_command = get_sql_command(config_path)

    if sql_query is not "" and csv_elements and sql_command is "":
        # Define the new python module to create
        new_module = f"""from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.udf import udf
import pandas as pd

# Set batch mode setting for optimiued batch processing. This may be changed later for stream processing
settings = EnvironmentSettings.in_batch_mode()
        
# Create TableEnvironment which is the central object used in the Table/SQL API
t_env = TableEnvironment.create(settings)

pdf = pd.read_csv('data.csv', sep='{seperator}', names={schema})
# write all the data to one file
t_env.get_config().get_configuration().set_string("parallelism.default", "1")
table = t_env.from_pandas(pdf, {schema})
t_env.create_temporary_view('{table_name}', table)
tab = t_env.sql_query('''{sql_query}''')

pd_table = tab.to_pandas()
pd_table.columns = {schema}
pd_table.to_csv("{csv_output_path}" + "output.csv", index=False)
                """

    elif map_function is not "":
        # Define the new python module to create
        new_module = f"""
from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.udf import udf
import pandas as pd
    
    
{map_function}
    
# Set batch mode setting for optimiued batch processing. This may be changed later for stream processing
settings = EnvironmentSettings.in_batch_mode()
    
# Create TableEnvironment which is the central object used in the Table/SQL API
t_env = TableEnvironment.create(settings)
    
pdf = pd.read_csv('data.csv', sep='{seperator}', names={schema})
# write all the data to one file
t_env.get_config().get_configuration().set_string("parallelism.default", "1")
table = t_env.from_pandas(pdf, {schema})
t_env.create_temporary_view('{table_name}', table)
tab = t_env.from_path("{table_name}")
    
map_function = udf({title},
                    result_type=DataTypes.ROW(
                        {type_list}),
                    func_type="pandas")
    
pd_table = tab.map(map_function).to_pandas()
pd_table.columns = {schema}
pd_table.to_csv("{csv_output_path}" + "output.csv", index=False)
            """

    elif sql_command is not "" and csv_elements:
        # Define the new python module to create
        new_module = f"""from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.udf import udf
import pandas as pd

# Set batch mode setting for optimiued batch processing. This may be changed later for stream processing
settings = EnvironmentSettings.in_batch_mode()
        
# Create TableEnvironment which is the central object used in the Table/SQL API
t_env = TableEnvironment.create(settings)
pdf = pd.read_csv('data.csv', sep='{seperator}', names={schema})
# write all the data to one file
t_env.get_config().get_configuration().set_string("parallelism.default", "1")
table = t_env.from_pandas(pdf, {schema})
t_env.create_temporary_view('{table_name}', table)
{sql_command}.wait()
                """
    elif sql_command is not "" and not csv_elements:
        # Define the new python module to create
        new_module = f"""from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.udf import udf
import pandas as pd

# Set batch mode setting for optimiued batch processing. This may be changed later for stream processing
settings = EnvironmentSettings.in_batch_mode()

# Create TableEnvironment which is the central object used in the Table/SQL API
t_env = TableEnvironment.create(settings)

# write all the data to one file
t_env.get_config().get_configuration().set_string("parallelism.default", "1")
{sql_command}.wait()"""
    else:
        new_module = ""

    # Write the new module
    file = open("input/output.py", "w")
    file.write(new_module)
    file.close()
    make_archive("output", "zip", "input")
