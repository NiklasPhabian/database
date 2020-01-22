An sql database abstraction
# Install
## From github
    pip install git+https://github.com/NiklasPhabian/database
        
## from local
    mkvirtualenv --python=/usr/bin/python3 $PROJECT_ENV

    pip3 install psycopg2
    pip3 install pandas
    pip3 install sqlalchemy

    git clone git@github.com:NiklasPhabian/database.git $DB_DIR
    pip3 install -e $DB_DIR

