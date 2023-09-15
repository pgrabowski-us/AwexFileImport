import pymysql
import os
import json

with open('dbConfig.json', 'r') as f:
    config = json.load(f)

try:
    # Nawiązanie połączenia z bazą danych
    conn = pymysql.connect(host=config["host"], user=config["username"], password=config["password"], db=config["database"], port=config["port"])
    cursor = conn.cursor()

    taskOffset = config.get("taskOffset", 0)  # Pobierz ostatnią zapisaną wartość offset, jeśli istnieje
    limit = 100

    while True:
        query = f"""
        SELECT files.SharedFileName,
            files.SharedFileSystemName,
            files.SharedFileBody,
            taskFileRel.TaskId,
            task.TaskProjectId
        FROM itcube.sharedfile files 
        JOIN itcube.tasksharedfilerel taskFileRel ON files.SharedFileId = taskFileRel.SharedFileId
        JOIN itcube.task task ON taskFileRel.TaskId = task.TaskId
        LIMIT {limit} OFFSET {taskOffset}
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            break

    # Zapisywanie plików
        for row in rows:
            SharedFileName, SharedFileSystemName, SharedFileBody, TaskId, TaskProjectId = row
            folder_path = os.path.join("investment", str(TaskProjectId), "task", str(TaskId))  # Konwersja SharedFileId na string
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
            with open(os.path.join(folder_path, f"{SharedFileSystemName.replace(' ', '-')}.{SharedFileName}"), "wb") as f:
                f.write(SharedFileBody)
            print(TaskId, " ", SharedFileSystemName)
            print("investment\\", str(TaskProjectId), "\\", "task\\", str(TaskId))

        taskOffset += limit
        config["taskOffset"] = taskOffset  # Aktualizuj wartość offset w config

        # Zapisz zaktualizowany config jako JSON
        with open('dbConfig.json', 'w') as f:
            json.dump(config, f)

except Exception as e:
    print(f"Wystąpił błąd: {e}")
finally:
    # Zamknięcie połączenia z bazą danych
    if cursor:
        cursor.close()
    if conn:
        conn.close()
