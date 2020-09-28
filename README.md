# Install


    mkvirtualenv --python=/usr/bin/python3 $PROJECT_ENV

    pip3 install psycopg2
    pip3 install pandas
    pip3 install sqlalchemy

    git clone git@github.com:NiklasPhabian/database.git $DB_DIR
    pip3 install -e $DB_DIR


# Usage
    import database
    
    db = database.PostgresDatabase(config_file='path/to/db_config.py', config_name='mydbserver')
    table = database.DBTable(database=db, table_name='customers, schema='public')    
    df = table.to_dataframe()
