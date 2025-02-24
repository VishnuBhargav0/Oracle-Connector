import oracledb

ORACLE_CONFIG = {
    "user": "username",
    "password": "password",
    "dsn": "host_url/service_name"}

def get_oracle_connection(ORACLE_CONFIG):
    """
    Creates a connection wiht the Oracle database using the ORACLE_CONFIG.
    
    Parameters:
       Oracle config as a dictionary, that contains the user, password, and dsn.
    Returns:
        Connection object.
    """

    print("--------------------------------------------")
    print("CONNECTION REQUEST INITIATED")
    connection = oracledb.connect(**ORACLE_CONFIG)
    print("CONNECTION HAS BEEN ESTABLISHED")
    print("--------------------------------------------")
    return connection

connection = get_oracle_connection()
cursor = connection.cursor()

# result = cursor.execute("SELECT * FROM contacts")
# rows = result.fetchall()
# for row in rows:
#    print(row)