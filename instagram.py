import pandas as pd
import numpy as np
import psycopg2
import json

file_path_follow_me = '/Users/khalilfarouqi/Downloads/instagram-farouqi_khalil-2023-12-06-vzx0jfVz/followers_and_following/followers_1.json'  # Follow me
file_path_follow_I = '/Users/khalilfarouqi/Downloads/instagram-farouqi_khalil-2023-12-06-vzx0jfVz/followers_and_following/following.json'  # I Follow
with open(file_path_follow_me, 'r') as file:
    json_data_follow_me = file.read()
with open(file_path_follow_I, 'r') as file:
    json_data_follow_I = file.read()

# Parse the JSON data
data_follow_me = json.loads(json_data_follow_me)
data_follow_I = json.loads(json_data_follow_I)

# Extract values and create a table
table_data_follow_me = []
for item in data_follow_me:
    for entry in item.get('string_list_data', []):
        table_data_follow_me.append(entry.get('value', ''))

table_data_follow_I = []
for item in data_follow_I['relationships_following']:
    for string_data in item['string_list_data']:
        table_data_follow_I.append(string_data['value'])

# Create a DataFrame and display as a table using pandas
df_follow_me = pd.DataFrame(table_data_follow_me)
df_follow_I = pd.DataFrame(table_data_follow_I)

conn = None

try:
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        dbname="Instagram analytics",
        user="postgres",
        password="khalil",
        host="localhost",
        port="5432"
    )

    # Create a cursor object using the cursor() method
    cursor = conn.cursor()

    # Execute a SQL query
    cursor.execute("delete from followers_Temp where id is not null;")
    conn.commit()
    for item in table_data_follow_I:
        cursor.execute("INSERT INTO followers_Temp VALUES (NEXTVAL('followers_Temp_id_seq'), '" + item + "', CURRENT_DATE);")
        conn.commit()

    cursor.execute("delete from followings_Temp where id is not null;")
    conn.commit()
    for item in table_data_follow_me:
        cursor.execute("INSERT INTO followings_Temp VALUES (NEXTVAL('followings_Temp_id_seq'), '" + item + "', CURRENT_DATE);")
        conn.commit()

    cursor.execute("INSERT INTO NotFollowing (followee_id, created_on) SELECT id, CURRENT_DATE FROM Followers WHERE username NOT IN (SELECT Fr.username FROM Followings Fr) and id Not in (select NF.followee_id from NotFollowing NF) ORDER BY id;")
    conn.commit()

    cursor.execute("INSERT INTO unfollowed (username, created_on) select f.username, CURRENT_DATE from followers f where f.username not in (select ft.username from followers_Temp ft);")
    conn.commit()

    cursor.execute("INSERT INTO lostfollow (username, created_on) select f.username, CURRENT_DATE from followings f where f.username not in (select ft.username from followings_Temp ft);")
    conn.commit()

    cursor.execute("INSERT INTO newfollowers (username, created_on) select ft.username, CURRENT_DATE from followers_Temp ft where ft.username not in (select username from followers);")
    conn.commit()

    cursor.execute("INSERT INTO newfollowing (username, created_on) select ft.username, CURRENT_DATE from followings_Temp ft where ft.username not in (select username from followings);")
    conn.commit()

    cursor.execute("INSERT INTO followers (id, username, created_on) select NEXTVAL('followers_id_seq'), username, CURRENT_DATE from newfollowers where created_on = CURRENT_DATE;")
    conn.commit()

    cursor.execute("INSERT INTO followings (id, username, created_on) select NEXTVAL('followings_id_seq'), username, CURRENT_DATE from newfollowing where created_on = CURRENT_DATE;")
    conn.commit()

    cursor.execute("delete from followers where username in (select username from unfollowed);")
    conn.commit()

    cursor.execute("delete from followings where username in (select username from lostfollow);")
    conn.commit()
    # Fetch result

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    # Close the cursor and connection
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed")
