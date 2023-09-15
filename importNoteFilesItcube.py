import pymysql
import os
import json

with open('dbConfig.json', 'r') as f:
    config = json.load(f)

try:
    # Nawiązanie połączenia z bazą danych
    conn = pymysql.connect(host=config["host"], user=config["username"], password=config["password"], db=config["database"], port=config["port"])
    cursor = conn.cursor()

    noteOffset = config.get("noteOffset", 0)  # Pobierz ostatnią zapisaną wartość offset, jeśli istnieje
    limit = 100

    while True:

        # Pobranie danych
        query = f"""
        select files.SharedFileName,
       files.SharedFileSystemName,
       files.SharedFileBody,
       notefile.NoteId,
       note.NoteProjectId
        FROM itcube.sharedfile files join itcube.notesharedfilerel notefile on files.SharedFileId = notefile.SharedFileId
        JOIN itcube.note note on notefile.NoteId = note.NoteId
        where notefile.NoteId
        LIMIT {limit} OFFSET {noteOffset}
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            break

    # Zapisywanie plików
        for row in rows:
            SharedFileName, SharedFileSystemName, SharedFileBody, NoteId, NoteProjectId = row
            folder_path = os.path.join("investment", str(NoteProjectId), "note", str(NoteId))  # Konwersja SharedFileId na string
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
            with open(os.path.join(folder_path, f"{SharedFileSystemName.replace(' ', '-')}.{SharedFileName}"), "wb") as f:
                f.write(SharedFileBody)
            print(NoteId, " ", SharedFileSystemName)
            print("investment\\", str(NoteProjectId), "\\", "note\\", str(NoteId))

        noteOffset += limit
        config["noteOffset"] = noteOffset  # Aktualizuj wartość offset w config

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
