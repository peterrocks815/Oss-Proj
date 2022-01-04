import datetime
import sys
import numpy
import pandas as pd
from shutil import make_archive


def is_json_data(path):
    file = open(path, "r")
    for line in file:
        if "JSON: " in line:
            tmp = line[6:-1]
            if "true" in tmp.lower():
                return True
    return False


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
    return command


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


def get_csv_elements_json(path, schema):
    df = pd.read_json(path)
    return_type_list = "["
    if df.size != 0:
        if df.shape[1] == 1:
            counter = 0
            for entry in df[df.keys()[0]][0]:
                if type(df[df.keys()[0]][0][entry]) == float:
                    return_type_list += "DataTypes.FIELD('" + schema[counter] + "', DataTypes.FLOAT())"
                elif type(df[df.keys()[0]][0][entry]) == type(""):
                    return_type_list += "DataTypes.FIELD('" + schema[counter] + "', DataTypes.STRING())"
                elif type(df[df.keys()[0]][0][entry]) == int:
                    return_type_list += "DataTypes.FIELD('" + schema[counter] + "', DataTypes.BIGINT())"
                elif type(df[df.keys()[0]][0][entry]) == bool:
                    return_type_list += "DataTypes.FIELD('" + schema[counter] + "', DataTypes.BOOLEAN())"
                elif type(df[df.keys()[0]][0][entry]) == datetime.datetime:
                    return_type_list += "DataTypes.FIELD('" + schema[counter] + "', DataTypes.DATE())"
                else:
                    print("Type not supported")
                    return False, None
                counter += 1
                return_type_list += ", "
            return_type_list += "]"
        else:
            return False, None
        return True, return_type_list
    else:
        return False, None


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

    # Get schema from schema file
    schema = get_schema(schema_path, seperator)

    # Check if the input data is in json format
    is_json = is_json_data(config_path)

    if is_json:
        # Get elements as well as the column types from the json file
        json_elements, type_list = get_csv_elements_json(data_path, schema)
        if type_list is None:
            return
    else:
        # Get elements as well as the column types from the csv file
        csv_elements, type_list = get_csv_elements(data_path, seperator, schema)

    # Get the function from the config file
    map_function = get_map_function(config_path)

    # Get the title of the function from the config file
    title = get_function_title(config_path)

    # Get the general flink configurations
    configurations = get_configurations(config_path)

    # Get the title of the function from the config file
    table_name = get_table_name(config_path)
    if table_name is "":
        table_name = "source_table"

    # Get the title of the function from the config file
    sql_query = get_sql_query(config_path)

    sql_command = get_sql_command(config_path)
    if is_json:
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
from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes
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
    else:
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
