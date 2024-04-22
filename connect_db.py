import cx_Oracle
import pandas as pd
import json
import pymongo

def fetch_from_oracle(sql_query, excel_filename):
    
    with open('db_creds.json') as f:
        db_creds = json.load(f)
    
    
    oracle_username = db_creds["oracle_username"]
    oracle_password = db_creds["oracle_password"]
    oracle_host = db_creds["oracle_host"]
    oracle_port = db_creds["oracle_port"]
    oracle_service_name = db_creds["oracle_service_name"]

    
    connection = cx_Oracle.connect(
        user=oracle_username,
        password=oracle_password,
        dsn=f"{oracle_host}:{oracle_port}/{oracle_service_name}"
    )


    cursor = connection.cursor()
    cursor.execute(sql_query)

    
    columns = [col[0] for col in cursor.description]
    result = cursor.fetchall()
    df = pd.DataFrame(result, columns=columns)

    
    cursor.close()
    connection.close()

    
    df.to_excel(excel_filename, index=False)
    print(f"Data from Oracle saved to {excel_filename}")

def fetch_data(database_name, query, output_file):
    
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[database_name]
    result = db.movies.aggregate(query)
    data = list(result)

    
    df = pd.DataFrame(data)

    
    df['wins'] = df['awards'].apply(lambda x: x.get('wins', None))
    df['nominations'] = df['awards'].apply(lambda x: x.get('nominations', None))
    df['imdb rating'] = df['imdb'].apply(lambda x: x.get('rating', None))
    df['cast'] = df['cast'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    df['directors'] = df['directors'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    df.drop(['awards', 'imdb'], axis=1, inplace=True)

    
    df.to_excel(output_file, index=False)
    print("Data from MongoDB exported to Excel successfully!")


database_choice = input("Which database? (1 for Oracle SQL, 2 for MongoDB): ")

if database_choice == "1":
    
    sql_query = """
    SELECT C.CUST_ID, C.CUST_FIRST_NAME, C.CUST_LAST_NAME, SUM(S.AMOUNT_SOLD) AS TOTAL_SALES
    FROM CUSTOMER C
    JOIN SALES S ON C.CUST_ID = S.CUST_ID
    GROUP BY C.CUST_ID, C.CUST_FIRST_NAME, C.CUST_LAST_NAME
    ORDER BY TOTAL_SALES DESC
    FETCH FIRST 10 ROWS ONLY
    """
    excel_filename = "top_clients.xlsx"
    fetch_from_oracle(sql_query, excel_filename)

elif database_choice == "2":
    
    database_name = "MOVIE"
    query = [
        {
            "$match": {
                "year": { "$gte": 2000 },
                "imdb.rating": { "$gte": 6.0 }
            }
        },
        {
            "$project": {
                "_id": 1,
                "plot": 1,
                "cast": 1,
                "directors": 1,
                "awards": 1,
                "imdb": 1
            }
        }
    ]
    output_file = "movies_data.xlsx"
    fetch_data(database_name, query, output_file)

else:
    print("Invalid choice! Please enter 1 or 2 for Oracle SQL or MongoDB.")
