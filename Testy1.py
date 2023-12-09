from pymongo import MongoClient
import mysql.connector
import redis
import json
import os

# MongoDB setup
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['PS3Games']
mongo_collection = mongo_db['Games']

# MySQL setup
mysql_password = os.getenv('MYSQL_PASSWORD')
mysql_connection = mysql.connector.connect(
    host='sql11.freemysqlhosting.net',
    user='sql11646759',
    password=mysql_password,
    database='sql11646759'
)
mysql_cursor = mysql_connection.cursor()

# Redis setup
redis_connection = redis.StrictRedis(host='localhost', port=6379, db=0)

# Function to browse records
def browse_records(platform):
    if platform in ['PS3', 'PS4', 'PS5']:
        print(f"Selected platform: {platform}")
        if platform == 'PS3':
            try:
                for game in mongo_collection.find({}, {'_id': 0}):
                    print(game)
            except Exception as e:
                print(f"Error accessing MongoDB: {e}")
        elif platform == 'PS4':
            query = f"SELECT * FROM Games{platform}"
            mysql_cursor.execute(query)
            records = mysql_cursor.fetchall()
            for record in records:
                print(record)
        elif platform == 'PS5':
            key = platform + 'Games'
            games = redis_connection.lrange(key, 0, -1)
            for game in games:
                try:
                    game_data = json.loads(game.decode('utf-8'))
                    print(game_data)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
    else:
        print("Invalid platform.")

# Function to add a new record
def add_record(platform):
    if platform in ['PS3', 'PS4', 'PS5']:
        if platform == 'PS3':
            new_game = {
                "Platform": input("Enter Platform: "),
                "Title": input("Enter Title: "),
                "Genre": input("Enter Genre: "),
                "Release Year": input("Enter Release Year: ")
            }
            try:
                mongo_collection.insert_one(new_game)
                print("Record added successfully!")
            except Exception as e:
                print(f"Error adding record to MongoDB: {e}")
        elif platform == 'PS4':
            platform = 'PS4'
            title = input("Enter Title: ")
            genre = input("Enter Genre: ")
            release_year = input("Enter Release Year: ")

            query = f"INSERT INTO Games{platform} (Platform, Title, Genre, ReleaseYear) VALUES (%s, %s, %s, %s)"
            values = (platform, title, genre, release_year)
            mysql_cursor.execute(query, values)
            mysql_connection.commit()
            print("Record added successfully!")
        elif platform == 'PS5':
            key = platform + 'Games'
            new_game = {
                "Platform": input("Enter Platform: "),
                "Title": input("Enter Title: "),
                "Genre": input("Enter Genre: "),
                "Release Year": input("Enter Release Year: ")
            }
            json_data = json.dumps(new_game)
            redis_connection.rpush(key, json_data)
            print("Record added successfully!")
    else:
        print("Invalid platform.")

# Function to remove a record
def remove_record(platform):
    if platform in ['PS3', 'PS4', 'PS5']:
        if platform == 'PS3':
            title_to_remove = input("Enter the Title of the game to remove: ").strip()
            try:
                result = mongo_collection.delete_one({"Title": title_to_remove})
                if result.deleted_count > 0:
                    print(f"Record '{title_to_remove}' removed successfully!")
                else:
                    print(f"No record found with the title '{title_to_remove}'.")
            except Exception as e:
                print(f"Error removing record from MongoDB: {e}")
        elif platform == 'PS4':
            title_to_remove = input("Enter Title of the game to remove: ")
            query = f"DELETE FROM GamesPS4 WHERE Title = %s"
            value = (title_to_remove,)
            mysql_cursor.execute(query, value)
            mysql_connection.commit()
            if mysql_cursor.rowcount > 0:
                print(f"Record '{title_to_remove}' removed successfully!")
            else:
                print(f"No record found with the title '{title_to_remove}'.")
        elif platform == 'PS5':
            key = platform + 'Games'
            title_to_remove = input("Enter the Title of the game to remove: ")

            games = redis_connection.lrange(key, 0, -1)
            for game in games:
                game_data = json.loads(game.decode('utf-8'))
                print(f"Comparing '{title_to_remove}' with '{game_data['Title']}'")
                if game_data['Title'] == title_to_remove:
                    redis_connection.lrem(key, 0, game)
                    print(f"Record '{title_to_remove}' removed successfully!")
                    break
            else:
                print(f"No record found with the title '{title_to_remove}'.")
        else:
            print("Invalid platform.")

# Function to adjust a record

def adjust_record(platform):
    if platform in ['PS3', 'PS4', 'PS5']:
        title_to_adjust = input("Enter the Title of the game to adjust: ").strip()
        record_found = False

        # Check if the title exists and set record_found to True if found
        if platform == 'PS3':
            result = mongo_collection.find_one({"Title": title_to_adjust})
            if result:
                record_found = True
            else:
                print(f"No record found with the title '{title_to_adjust}'.")
        elif platform == 'PS4':
            query = f"SELECT * FROM Games{platform} WHERE Title = %s"
            value = (title_to_adjust,)
            mysql_cursor.execute(query, value)
            if mysql_cursor.fetchone():
                record_found = True
            else:
                print(f"No record found with the title '{title_to_adjust}'.")
        elif platform == 'PS5':
            key = platform + 'Games'
            games = redis_connection.lrange(key, 0, -1)
            for game in games:
                game_data = json.loads(game.decode('utf-8'))
                if game_data['Title'] == title_to_adjust:
                    record_found = True
                    break
            if not record_found:
                print(f"No record found with the title '{title_to_adjust}'.")


        if record_found:

            if platform == 'PS3':
                try:
                    updated_values = {
                        "$set": {
                            "Platform": input("Enter new Platform: "),
                            "Title": input("Enter new Title: "),
                            "Genre": input("Enter new Genre: "),
                            "Release Year": input("Enter new Release Year: ")
                        }
                    }
                    result = mongo_collection.update_one({"Title": title_to_adjust}, updated_values)
                    if result.modified_count > 0:
                        print(f"Record '{title_to_adjust}' adjusted successfully!")
                    else:
                        print(f"Error adjusting the record '{title_to_adjust}'.")
                except Exception as e:
                    print(f"Error adjusting record in MongoDB: {e}")

            elif platform == 'PS4':
                new_platform = input("Enter new Platform: ")
                new_title = input("Enter new Title: ")
                new_genre = input("Enter new Genre: ")
                new_release_year = input("Enter new Release Year: ")

                query = f"UPDATE Games{platform} SET Platform = %s, Title = %s, Genre = %s, ReleaseYear = %s WHERE Title = %s"
                values = (new_platform, new_title, new_genre, new_release_year, title_to_adjust)
                mysql_cursor.execute(query, values)
                mysql_connection.commit()
                if mysql_cursor.rowcount > 0:
                    print(f"Record '{title_to_adjust}' adjusted successfully!")
                else:
                    print(f"Error adjusting the record '{title_to_adjust}'.")

            elif platform == 'PS5':
                key = platform + 'Games'
                new_game = {
                    "Platform": input("Enter new Platform: "),
                    "Title": input("Enter new Title: "),
                    "Genre": input("Enter new Genre: "),
                    "Release Year": input("Enter new Release Year: ")
                }
                updated_json_data = json.dumps(new_game)
                games = redis_connection.lrange(key, 0, -1)
                for game in games:
                    game_data = json.loads(game.decode('utf-8'))
                    if game_data['Title'] == title_to_adjust:
                        redis_connection.lrem(key, 0, game)
                        redis_connection.rpush(key, updated_json_data)
                        print(f"Record '{title_to_adjust}' adjusted successfully!")
                        break
                else:
                    print(f"Error adjusting the record '{title_to_adjust}'.")
        else:
            print("No record found. Cannot adjust.")

    else:
        print("Invalid platform.")

# Main function
def main():
    while True:
        print("\nMenu:")
        print("1. Browse Records")
        print("2. Add Record")
        print("3. Remove Record")
        print("4. Adjust Record")
        print("5. Exit")
        choice = input("Enter your choice: ")

        if choice == "1":
            platform = input("Choose platform (PS3, PS4, PS5): ").upper()
            browse_records(platform)
        elif choice == "2":
            platform = input("Choose platform (PS3, PS4, PS5): ").upper()
            add_record(platform)
        elif choice == "3":
            platform = input("Choose platform (PS3, PS4, PS5): ").upper()
            remove_record(platform)
        elif choice == "4":
            platform = input("Choose platform (PS3, PS4, PS5): ").upper()
            adjust_record(platform)
        elif choice == "5":
            break
        else:
            print("Invalid choice. Please try again.")

    # Close connections
    mongo_client.close()
    mysql_cursor.close()
    mysql_connection.close()

if __name__ == "__main__":
    main()
