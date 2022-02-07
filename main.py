import datetime
import sys
import numpy
import os
import codecs
import pandas as pd
from shutil import make_archive


def process_variables(path):
    file = open(path, "r")
    new_file = ""
    variables = []
    env_vars = []
    for line in file:
        if "list_of_vars: " in line:
            variables = line[15:-2]
            variables = variables.split(", ")
            for var in variables:
                env_var = os.getenv(var)
                env_vars.append(env_var)
    file.close()
    file = open(path, "r")
    for line in file:
        new_line = line
        if "list_of_vars: " in line:
            new_file += new_line
            continue
        for i in range(len(env_vars)):
            if env_vars[i] is None:
                print("Environment variable: " + variables[i] + " not found")
            else:
                new_line = new_line.replace(variables[i], env_vars[i])
        new_file += new_line
    file.close()
    file = open(path, "w")
    file.write(new_file)
    file.close()


def get_data_format(path):
    file = open(path, "r")
    for line in file:
        if "input_type: " in line:
            tmp = line[12:-1]
            file.close()
            return tmp.lower()
    file.close()
    return None


def get_configurations(path):
    file = open(path, "r")
    command = ""
    for line in file:
        if "pipeline.auto-generate-uids: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:27]
            command += "', '" + line[29:-1] + "')\n"

        if "pipeline.auto-type-registration: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:31]
            command += "', '" + line[33:-1] + "')\n"

        if "pipeline.auto-watermark-interval: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:32]
            command += "', '" + line[34:-1] + "')\n"

        if "pipeline.cached-files: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:21]
            command += "', '" + line[23:-1] + "')\n"

        if "pipeline.classpaths: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:19]
            command += "', '" + line[21:-1] + "')\n"

        if "pipeline.closure-cleaner-level: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:30]
            command += "', '" + line[32:-1] + "')\n"

        if "pipeline.default-kryo-serializers: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:33]
            command += "', '" + line[35:-1] + "')\n"

        if "pipeline.force-avro: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:19]
            command += "', '" + line[21:-1] + "')\n"

        if "pipeline.force-kryo: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:19]
            command += "', '" + line[21:-1] + "')\n"

        if "pipeline.generic-types: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:22]
            command += "', '" + line[24:-1] + "')\n"

        if "pipeline.global-job-parameters: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:30]
            command += "', '" + line[32:-1] + "')\n"

        if "pipeline.jars: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:13]
            command += "', '" + line[15:-1] + "')\n"

        if "pipeline.max-parallelism: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:24]
            command += "', '" + line[26:-1] + "')\n"

        if "pipeline.name: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:13]
            command += "', '" + line[15:-1] + "')\n"

        if "pipeline.object-reuse: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:21]
            command += "', '" + line[23:-1] + "')\n"

        if "pipeline.operator-chaining: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:26]
            command += "', '" + line[28:-1] + "')\n"

        if "pipeline.registered-kryo-types: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:30]
            command += "', '" + line[32:-1] + "')\n"

        if "pipeline.registered-pojo-types: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:30]
            command += "', '" + line[32:-1] + "')\n"

        if "pipeline.time-characteristic: " in line:
            command += "t_env.get_config().get_configuration().set_string('" + line[:28]
            command += "', '" + line[30:-1] + "')\n"
    file.close()
    return command


def get_sql_command(path):
    file = open(path, "r")
    command = "t_env.execute_sql('''"
    new_command = False
    for line in file:
        if "sql_command: " in line:
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
    file.close()
    return command


def get_sql_query(path):
    file = open(path, "r")
    query = ""
    for line in file:
        if "sql_query: " in line:
            query += line[10:]
            for l in file:
                if ";" in l:
                    l = l.replace(";", "")
                if l is "\n" or l is "\r":
                    file.close()
                    return query
                query += l
    file.close()
    return query


def get_table_name(path):
    file = open(path, "r")
    name = ""
    for line in file:
        if "table_name: " in line:
            line = line.replace("\n", "")
            line = line.replace("\r", "")
            name += line[12:]
    file.close()
    return name


def get_function_title(path):
    file = open(path, "r")
    call = ""
    for line in file:
        if "function_title: " in line:
            call += line[16:]
    file.close()
    return call


def get_map_function(path):
    file = open(path, "r")
    fun = ""
    for line in file:
        if "function: " in line:
            fun += line[10:]
            for l in file:
                if l is "\n" or l is "\r":
                    file.close()
                    return fun
                fun += l
    file.close()
    return fun


def get_seperator(path):
    file = open(path, "r")
    sep = ""
    for line in file:
        if "seperator: " in line:
            sep = line[11:].strip()
            sep = codecs.decode(sep, 'unicode_escape')
    file.close()
    return sep


def get_json_elements(path, schema):
    df = pd.read_json(path)
    return_type_list = "["
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
    return True, return_type_list


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
    file.close()
    return header


def create_output(data_path=None, schema_path=None, config_path=None):
    # Sanity checks
    if data_path is None:
        print("please specify a data csv file")
        return
    if schema_path is None:
        print("please specify a schema as a .txt file")
        return
    if config_path is None:
        print("please specify a config_sql_command file")
        return

    # Replace all custom variables with their values
    process_variables(config_path)

    # Get seperator from config_sql_command file
    seperator = get_seperator(config_path)

    # Get schema from schema file
    schema = get_schema(schema_path, seperator)

    # Check if the input data is in json format
    data_format = get_data_format(config_path)

    if data_format == "json":
        # Get elements as well as the column types from the json file
        json_elements, type_list = get_json_elements(data_path, schema)
        if type_list is None:
            return
    elif data_format == 'csv':
        # Get elements as well as the column types from the csv file
        csv_elements, type_list = get_csv_elements(data_path, seperator, schema)

    # Get the function from the config_sql_command file
    map_function = get_map_function(config_path)

    # Get the title of the function from the config_sql_command file
    title = get_function_title(config_path)

    # Get the general flink configurations
    configurations = get_configurations(config_path)

    # Get the title of the function from the config_sql_command file
    table_name = get_table_name(config_path)
    if table_name is "":
        table_name = "source_table"

    # Get the title of the function from the config_sql_command file
    sql_query = get_sql_query(config_path)

    sql_command = get_sql_command(config_path)
    if data_format == "json":
        if sql_query is not "" and json_elements and sql_command is "":
            # Define the new python module to create
            new_module = f"""from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.udf import udf
import pandas as pd

# Set batch mode setting for optimiued batch processing. This may be changed later for stream processing
settings = EnvironmentSettings.in_batch_mode()

# Create TableEnvironment which is the central object used in the Table/SQL API
t_env = TableEnvironment.create(settings)

pdf = pd.read_json('data.json')
# write all the data to one file
t_env.get_config().get_configuration().set_string("parallelism.default", "1")
{configurations}
table = t_env.from_pandas(pdf, {schema})
t_env.create_temporary_view('{table_name}', table)
tab = t_env.sql_query('''{sql_query}''')

pd_table = tab.to_pandas()
pd_table.columns = {schema}
pd_table.to_csv("output.csv", index=False)
                """
            if sql_query is not "" and json_elements and sql_command is not "":
                # Define the new python module to create
                new_module = f"""from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
from pyflink.table.udf import udf
import pandas as pd

# Set batch mode setting for optimiued batch processing. This may be changed later for stream processing
settings = EnvironmentSettings.in_batch_mode()

# Create TableEnvironment which is the central object used in the Table/SQL API
t_env = TableEnvironment.create(settings)

pdf = pd.read_json('data.json')
# write all the data to one file
t_env.get_config().get_configuration().set_string("parallelism.default", "1")
{configurations}
table = t_env.from_pandas(pdf, {schema})
t_env.create_temporary_view('{table_name}', table)
{sql_command}.wait()
tab = t_env.sql_query('''{sql_query}''')

pd_table = tab.to_pandas()
pd_table.columns = {schema}
pd_table.to_csv("output.csv", index=False)
                """

        elif map_function is not "" and json_elements:
            # Define the new python module to create
            new_module = f"""
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.udf import udf
import pandas as pd


{map_function}

# Set batch mode setting for optimiued batch processing. This may be changed later for stream processing
settings = EnvironmentSettings.in_batch_mode()

# Create TableEnvironment which is the central object used in the Table/SQL API
t_env = TableEnvironment.create(settings)

pdf = pd.read_json('data.json')
# write all the data to one file
t_env.get_config().get_configuration().set_string("parallelism.default", "1")
{configurations}
table = t_env.from_pandas(pdf, {schema})
t_env.create_temporary_view('{table_name}', table)
tab = t_env.from_path("{table_name}")

map_function = udf({title},
                    result_type=DataTypes.ROW(
                        {type_list}),
                    func_type="pandas")

pd_table = tab.map(map_function).to_pandas()
pd_table.columns = {schema}
pd_table.to_csv("output.csv", index=False)
            """

        elif sql_command is not "" and json_elements:
            # Define the new python module to create
            new_module = f"""from pyflink.table import EnvironmentSettings, TableEnvironment
import pandas as pd

# Set batch mode setting for optimiued batch processing. This may be changed later for stream processing
settings = EnvironmentSettings.in_batch_mode()

# Create TableEnvironment which is the central object used in the Table/SQL API
t_env = TableEnvironment.create(settings)
pdf = pd.read_json('data.json')
# write all the data to one file
t_env.get_config().get_configuration().set_string("parallelism.default", "1")
{configurations}
table = t_env.from_pandas(pdf, {schema})
t_env.create_temporary_view('{table_name}', table)
{sql_command}.wait()
                        """
        elif sql_command is not "" and not json_elements:
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
{configurations}
{sql_command}.wait()"""
        elif sql_command is not "" and sql_query is not "":
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
{configurations}
{sql_command}.wait()

tab = t_env.sql_query('''{sql_query}''')

pd_table = tab.to_pandas()
pd_table.columns = {schema}
pd_table.to_csv("output.csv", index=False)"""
        else:
            new_module = ""

        # Write the new module
        file = open("input/output.py", "w")
        file.write(new_module)
        file.close()
        make_archive("output", "zip", "input")
    elif data_format == "csv":
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
{configurations}
table = t_env.from_pandas(pdf, {schema})
t_env.create_temporary_view('{table_name}', table)
tab = t_env.sql_query('''{sql_query}''')

pd_table = tab.to_pandas()
pd_table.columns = {schema}
pd_table.to_csv("output.csv", index=False)
                """
        if sql_query is not "" and csv_elements and sql_command is not "":
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
{configurations}
table = t_env.from_pandas(pdf, {schema})
t_env.create_temporary_view('{table_name}', table)
{sql_command}.wait()
tab = t_env.sql_query('''{sql_query}''')

pd_table = tab.to_pandas()
pd_table.columns = {schema}
pd_table.to_csv("output.csv", index=False)
                """

        elif map_function is not "" and csv_elements:
            # Define the new python module to create
            new_module = f"""
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
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
{configurations}
table = t_env.from_pandas(pdf, {schema})
t_env.create_temporary_view('{table_name}', table)
tab = t_env.from_path("{table_name}")
    
map_function = udf({title},
                    result_type=DataTypes.ROW(
                        {type_list}),
                    func_type="pandas")
    
pd_table = tab.map(map_function).to_pandas()
pd_table.columns = {schema}
pd_table.to_csv("output.csv", index=False)
            """

        elif sql_command is not "" and csv_elements:
            # Define the new python module to create
            new_module = f"""from pyflink.table import EnvironmentSettings, TableEnvironment
import pandas as pd

# Set batch mode setting for optimiued batch processing. This may be changed later for stream processing
settings = EnvironmentSettings.in_batch_mode()
        
# Create TableEnvironment which is the central object used in the Table/SQL API
t_env = TableEnvironment.create(settings)
pdf = pd.read_csv('data.csv', sep='{seperator}', names={schema})
# write all the data to one file
t_env.get_config().get_configuration().set_string("parallelism.default", "1")
{configurations}
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
{configurations}
{sql_command}.wait()"""
        elif sql_command is not "" and sql_query is not "":
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
{configurations}
{sql_command}.wait()

tab = t_env.sql_query('''{sql_query}''')

pd_table = tab.to_pandas()
pd_table.columns = {schema}
pd_table.to_csv("output.csv", index=False)"""
        else:
            new_module = ""

        # Write the new module
        file = open("input/output.py", "w")
        file.write(new_module)
        file.close()
        make_archive("output", "zip", "input")


if __name__ == '__main__':
    create_output(sys.argv[1], sys.argv[2], sys.argv[3])
