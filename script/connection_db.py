import pyodbc as odbc

# Set up the connection string
driver = 'ODBC Driver 17 for SQL Server'
server = '169.254.26.93'
database = 'master'
username = 'hicham'
password = '123@123'

connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}" # UID={username};PWD={password}

# Connect to default database;
connection = odbc.connect(connection_string, autocommit=True)
cursor = connection.cursor()