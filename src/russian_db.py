# Load modules 
import os
import psycopg2

###############################################################################
# SQL Query Snippets
###############################################################################
if __name__ == '__main__':
    # Change working directory to location location of this script
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    # Instantiate psycopg2 connection to default db
    conn = psycopg2.connect(host=os.environ['AWS_RDS'],
                            port=5432, user=os.environ['MASTER_RDS_USERNAME'],
                            password=os.environ['MASTER_RDS_PASSWORD'])
    conn.autocommit=True
            
    c = conn.cursor() # instantiate cursor

    # Create database ira
    c.execute("CREATE DATABASE ira;") 

    # Write query to create user and make owner of ira db
    q = '''
    CREATE USER {} ENCRYPTED PASSWORD '{}';
    
    ALTER DATABASE ira OWNER TO {}; 
    '''.format(os.environ['IRA_USR'],
               os.environ['IRA_PWD'],
               os.environ['IRA_USR'])
    
    c.execute(q) # execute query
    
    conn.close() # close connection to default db

    # Instantiate connection to new db
    conn = psycopg2.connect(dbname='ira',
                            host=os.environ['AWS_RDS'],
                            port=5432, user=os.environ['IRA_USR'],
                            password=os.environ['IRA_PWD'])
    c = conn.cursor() # instantiate cursor

    # Write query to create tables 
    q = '''
    CREATE TABLE email_responses (
        external_author_id integer,
        author             varchar(15),
        content            text,
        region             varchar,
        language           varchar,
        publish_date       timestamp,
        harvested_date     timestamp,
        following          integer,
        followers          integer,
        updates            integer,
        post_type          varchar,
        account_type       varchar,
        retweet            boolean,
        account_category   varchar(12),
        "timestamp"        timestamp,
        action             varchar(5)
        );
    '''
    c.execute(q) # execute query

    # Open csv files and copy into postgres db
    for i in range(1,14):
        with open('../data/IRAhandle_tweets_{}.csv'.format(i), 'r') as f:
            next(f) # skip header
            c.copy_from(f, 'ira', sep=',')

    conn.commit()
    conn.close() # close connection
