import connection_db as conn


# Function to execute SQL statements
def execute_sql_statement(sql):
    conn.cursor.execute(sql)
    conn.connection.commit()
    
#######################################
# Create the Datawarehouse and use it #
#######################################

# Create Database MovieDB
sql = ('''
    IF EXISTS (SELECT name FROM sys.databases WHERE name = 'MovieDB')
        PRINT 'Database MovieDB exist'
    ELSE
        CREATE DATABASE MovieDB;
    
    USE MovieDB;
''')
execute_sql_statement(sql)

#####################################
# Functions To Create Tables for DW #
#####################################

# Function to create Production_Country table
def create_table_production_country():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Production_Country' and xtype='U')
        CREATE TABLE [Production_Country] (
        [prod_country_id] Varchar(10),
        [name] Varchar(55),
        PRIMARY KEY ([prod_country_id])
    )
    """
    execute_sql_statement(sql)

# Function to create Country table
def create_table_country():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Country' and xtype='U')
        CREATE TABLE [Country] (
        [country_id] Integer,
        [name] Varchar(255),
        PRIMARY KEY ([country_id])
        )
    """
    execute_sql_statement(sql)

# Function to create City table
def create_table_city():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='City' and xtype='U')
        CREATE TABLE [City] (
        [city_id] Integer,
        [name] Varchar(255),
        PRIMARY KEY ([city_id])
        )
    """
    execute_sql_statement(sql)

# Function to create Gender table
def create_table_gender():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Gender' and xtype='U')
        CREATE TABLE [Gender] (
        [gender_id] Integer,
        [name] Varchar(50),
        PRIMARY KEY ([gender_id])
        )
    """
    execute_sql_statement(sql)

# Function to create Department table
def create_table_department():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Department' and xtype='U')
        CREATE TABLE [Department] (
        [dep_id] Integer,
        [name] Varchar(255),
        PRIMARY KEY ([dep_id])
        )
    """
    execute_sql_statement(sql)

# Function to create Actor table
def create_table_actor():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Actor' and xtype='U')
        CREATE TABLE [Actor] (
        [actor_id] Integer,
        [gender_id] Integer,
        [dep_id] Integer,
        [country_id] Integer,
        [city_id] Integer,
        [full_name] varchar(255),
        [profile_path] Varchar(255),
        [birthday] Date,
        [deathday] Date,
        PRIMARY KEY ([actor_id]),
        CONSTRAINT [FK_Actor.country_id]
            FOREIGN KEY ([country_id])
            REFERENCES [Country]([country_id]),
        CONSTRAINT [FK_Actor.gender_id]
            FOREIGN KEY ([gender_id])
            REFERENCES [Gender]([gender_id]),
        CONSTRAINT [FK_Actor.dep_id]
            FOREIGN KEY ([dep_id])
            REFERENCES [Department]([dep_id]),
        CONSTRAINT [FK_Actor.city_id]
            FOREIGN KEY ([city_id])
            REFERENCES [City]([city_id])
        )
    """
    execute_sql_statement(sql)

# Function to create Movie table
def create_table_movie():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Movie' and xtype='U')
        CREATE TABLE [Movie] (
        [movie_id] Integer,
        [title] Varchar(255),
        [original_title] Varchar(255),
        [original_language] Varchar(5),
        [overview] Text,
        [poster_path] Varchar(255),
        [release_date] Date,
        PRIMARY KEY ([movie_id])
        )
    """
    execute_sql_statement(sql)

# Function to create Production_Company table
def create_table_production_company():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Production_Company' and xtype='U')
        CREATE TABLE [Production_Company] (
        [prod_company_id] Integer,
        [name] Varchar(255),
        [logo_path] Varchar(255),
        [origin_country] Varchar(10),
        PRIMARY KEY ([prod_company_id])
        )
    """
    execute_sql_statement(sql)

# Function to create Movie_Prod_Company table
def create_table_movie_prod_company():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Movie_Prod_Company' and xtype='U')
        CREATE TABLE [Movie_Prod_Company] (
        [movie_id] Integer,
        [prod_company_id] Integer,
        CONSTRAINT [FK_Movie_Prod_Company.movie_id]
            FOREIGN KEY ([movie_id])
            REFERENCES [Movie]([movie_id]),
        CONSTRAINT [FK_Movie_Prod_Company.prod_company_id]
            FOREIGN KEY ([prod_company_id])
            REFERENCES [Production_Company]([prod_company_id])
        )
    """
    execute_sql_statement(sql)

# Function to create Movie_Prod_Country table
def create_table_movie_prod_country():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Movie_Prod_Country' and xtype='U')
        CREATE TABLE [Movie_Prod_Country] (
        [movie_id] Integer,
        [prod_country_id] Varchar(10),
        CONSTRAINT [FK_Movie_Prod_Country.movie_id]
            FOREIGN KEY ([movie_id])
            REFERENCES [Movie]([movie_id]),
        CONSTRAINT [FK_Movie_Prod_Country.prod_country_id]
            FOREIGN KEY ([prod_country_id])
            REFERENCES [Production_Country]([prod_country_id])
        )
    """
    execute_sql_statement(sql)

# Function to create Genre table
def create_table_genre():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Genre' and xtype='U')
        CREATE TABLE [Genre] (
        [genre_id] Integer,
        [name] Varchar(55),
        PRIMARY KEY ([genre_id])
        )
    """
    execute_sql_statement(sql)

# Function to create Movie_Genre table
def create_table_movie_genre():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Movie_Genre' and xtype='U')
        CREATE TABLE [Movie_Genre] (
        [movie_id] Integer,
        [genre_id] Integer,
        CONSTRAINT [FK_Movie_Genre.movie_id]
            FOREIGN KEY ([movie_id])
            REFERENCES [Movie]([movie_id]),
        CONSTRAINT [FK_Movie_Genre.genre_id]
            FOREIGN KEY ([genre_id])
            REFERENCES [Genre]([genre_id])
        )
    """
    execute_sql_statement(sql)

# Function to create Realization table
def create_table_realization():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Realization' and xtype='U')
        CREATE TABLE [Realization] (
        [actor_id] Integer,
        [movie_id] Integer,
        [actor_popularity] Float,
        [movie_budget] BIGINT,
        [movie_revenue] BIGINT,
        [movie_popularity] Float,
        [movie_vote_average] Float,
        [movie_vote_count] BIGINT,
        CONSTRAINT [FK_realization.actor_id]
            FOREIGN KEY ([actor_id])
            REFERENCES [Actor]([actor_id]),
        CONSTRAINT [FK_realization.movie_id]
            FOREIGN KEY ([movie_id])
            REFERENCES [Movie]([movie_id])
        )
    """
    execute_sql_statement(sql)

####################################
#       Create tables of DW        #
####################################
    
create_table_genre()
create_table_production_company()
create_table_production_country()
create_table_movie()
create_table_movie_genre()
create_table_movie_prod_company()
create_table_movie_prod_country()
create_table_gender()
create_table_department()
create_table_country()
create_table_city()
create_table_actor()
create_table_realization()


# Create Database Movie_DM
sql = ('''
    IF EXISTS (SELECT name FROM sys.databases WHERE name = 'Movie_DM')
        PRINT 'Database Movie_DM exist'
    ELSE
        CREATE DATABASE Movie_DM;
    
    USE Movie_DM;
''')
execute_sql_statement(sql)

def create_table_movie_dm():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Movie' and xtype='U')
        CREATE TABLE [Movie] (
        [movie_id] Integer,
        [movie_budget] BIGINT,
        [movie_revenue] BIGINT,
        [movie_popularity] Float,
        [movie_vote_average] Float,
        [movie_vote_count] BIGINT,
        PRIMARY KEY ([movie_id])
        )
    """
    execute_sql_statement(sql)

def create_table_movie_detail():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Movie_Detail' and xtype='U')
        CREATE TABLE [Movie_Detail] (
        [movie_id] Integer,
        [title] Varchar(255),
        [original_title] Varchar(255),
        [original_language] Varchar(5),
        [overview] Text,
        [poster_path] Varchar(255),
        [release_date] Date,
        PRIMARY KEY ([movie_id]),
        CONSTRAINT [FK_Movie_Detail.movie_id]
            FOREIGN KEY ([movie_id])
            REFERENCES [Movie]([movie_id]),
        )
    """
    execute_sql_statement(sql)


####################################
#    Create tables of Movie DM     #
####################################
    
create_table_genre()
create_table_production_company()
create_table_production_country()
create_table_movie_dm()
create_table_movie_detail()
create_table_movie_genre()
create_table_movie_prod_company()
create_table_movie_prod_country()


# Create Database Actor_DM
sql = ('''
    IF EXISTS (SELECT name FROM sys.databases WHERE name = 'Actor_DM')
        PRINT 'Database Actor_DM exist'
    ELSE
        CREATE DATABASE Actor_DM;
    
    USE Actor_DM;
''')
execute_sql_statement(sql)

# Function to create Actor table
def create_table_actor_dm():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Actor' and xtype='U')
        CREATE TABLE [Actor] (
        [actor_id] Integer,
        [gender_id] Integer,
        [dep_id] Integer,
        [country_id] Integer,
        [city_id] Integer,
        [actor_popularity] Float,
        PRIMARY KEY ([actor_id]),
        CONSTRAINT [FK_Actor.country_id]
            FOREIGN KEY ([country_id])
            REFERENCES [Country]([country_id]),
        CONSTRAINT [FK_Actor.gender_id]
            FOREIGN KEY ([gender_id])
            REFERENCES [Gender]([gender_id]),
        CONSTRAINT [FK_Actor.dep_id]
            FOREIGN KEY ([dep_id])
            REFERENCES [Department]([dep_id]),
        CONSTRAINT [FK_Actor.city_id]
            FOREIGN KEY ([city_id])
            REFERENCES [City]([city_id])
        )
    """
    execute_sql_statement(sql)

# Function to create Actor table
def create_table_actor_detail():
    sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Actor_Detail' and xtype='U')
        CREATE TABLE [Actor_Detail] (
        [actor_id] Integer,
        [full_name] varchar(255),
        [profile_path] Varchar(255),
        [birthday] Date,
        [deathday] Date,
        PRIMARY KEY ([actor_id])
        )
    """
    execute_sql_statement(sql)


####################################
#    Create tables of Actor DM     #
####################################
    
create_table_gender()
create_table_department()
create_table_country()
create_table_city()
create_table_actor_detail()
create_table_actor_dm()


