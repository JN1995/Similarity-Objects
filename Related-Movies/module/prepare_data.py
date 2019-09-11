import pymssql 
import time 
import pandas as pd 
import logging

SQL_HOST='HOST_SERVER'
SQL_USER='username'
SQL_PWD='passwork'
SQL_DB='NAME_DB'
STORE="STORE_PROCEDURE"
_TYPE={1: "'VOD'", 2: "'RELAX'", 3: "'CHILD'"}

logging.basicConfig(filename='related.log', level=logging.INFO, filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def create_connect():
    """
    Create connect to mssql with some global parameter

    Input: no argument
    Output: sqlserver<pymssql.Connection>
    """
    connect = pymssql.connect(
                    server=SQL_HOST,
                    user=SQL_USER,
                    password=SQL_PWD,
                    database=SQL_DB,
                    as_dict=True,
                    charset='UTF-8'
                    )

    return connect

def get_data(_typeid=1):
    """
    Get data movie with @search: AppName

    Input: _typeid<int> is key in _TYPE global constant
            with default: 1 <-> "VOD"

    Output: result<list(dic)> list of movie with them properties

    Example:
        To get data for RELAX app:
            data = get_data(2)
    """
    start = time.time()

    connect_sql = create_connect()
    cursor = connect_sql.cursor()

    cursor.execute("%s %s"%(STORE, _TYPE.get(_typeid, _TYPE[1])))

    result = cursor.fetchall()

    logging.info("Finish get data of %s with time execute: %s"%(_TYPE[_typeid], time.time()-start))

    connect_sql.close()

    return result

def get_many_data():
    """
    GET many data with @search in _TYPE global constant

    Input:

    Ouput: result<list<dic>> concat all data movie of each app together
    """
    start = time.time()

    result = []
    for _typeid in _TYPE:
        result += get_data(_typeid)

    logging.info("Finish get many data with time execute: %s"%(time.time()-start))

    return result


def get_all_data():

    """
    Get data movie with @search: ALL

    Input: 

    Output: result<list(dic)> 

    """
    start = time.time()

    connect_sql = create_connect()
    cursor = connect_sql.cursor()

    cursor.execute("%s %s"%(STORE, "'ALL'"))

    result = cursor.fetchall()

    logging.info("Finish get all data with time execute: %s"%(time.time()-start))

    connect_sql.close()

    return result


def convert_to_pandas(data):
    """
    Convert data from list of dictionaries to dataframe pandas

    Input: data<list<dic>>

    Output: result<pandas.DataFrame> 
    """
    result = pd.DataFrame(data=data)

    return result


def remove_collums_df(df, *args, **kwargs):
    """

    """
    pass 

def handle_NAN():
    """

    """
    pass 


def handle_invalid_data():
    """

    """
    pass 


def preprocessing_data():
    """

    """
    pass


if __name__ == "__main__":

    # test
    # data = get_data()
    data = get_all_data()

    print(len(data))
