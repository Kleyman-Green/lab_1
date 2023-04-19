import uuid  # данная библеотека отвечает за хеширование
import os
import sqlite3

class FileSystem:
    DATA_PATH = f'{os.path.dirname(os.path.abspath(__file__))}/data.db'  # путь к датабазе - та же директория, что и программа

    def __init__(self):
        self.con = sqlite3.connect(self.DATA_PATH, check_same_thread=0, timeout=2)
        self.cur = self.con.cursor()
        self.cur.execute('''CREATE TABLE IF NOT EXISTS files(
                id TEXT PRIMARY KEY,   
                path TEXT,
                data BLOB)
                ''')
        self.con.commit()
    def get_file(self, id):
        return self.cur.execute(''' SELECT path FROM files WHERE id = ?''', [id]).fetchone()[0]
    def save_file(self, path):
        try:
            data = open(path, mode="rb").read()
        except (FileNotFoundError, IsADirectoryError):
            raise
        self.cur.execute('''INSERT OR IGNORE INTO files (id,path,data) VALUES(?,?,?)''',[id:=str(uuid.uuid5(uuid.NAMESPACE_DNS, path)),path,data])
        #используем uuid5 так как при использование uuid4 получалась генерация нового id каждый раз при запуске метода
        self.con.commit()
        return id

    def get_list(self, id_list: list[str]):
        res = ', '.join(['?' * len(id_list)]) # ?, ? , ... создает столько вопросиков, сколько длина списка
        self.cur.execute(f'SELECT path FROM files WHERE id IN (%s)' % res, id_list)
        return [i[0] for i in self.cur.fetchall()]

    def get_all(self):
        self.cur.execute('SELECT id,path FROM files')
        return self.cur.fetchall() # [[id1, path1], [id2, path2], [id3, path3]]

    def flush_file(self, id): # удаление всего по заданному id
        self.cur.execute('''DELETE FROM files WHERE id = ?''', [id])
        self.con.commit()

    def rename_file(self, oldid, newid):
        self.cur.execute('''UPDATE OR IGNORE files SET id = ? WHERE id = ?''', [newid, oldid])
        #ignore в случае если у нас есть запись с тем именем, на которое мы хотим заменить
        self.con.commit()
        return f"переименование на {newid} прошло успешно"

    def flush_all(self):
        self.cur.execute('''DROP TABLE files''')
system = FileSystem()
print(id :=system.save_file(r"C:\Users\Всеволод\Desktop\test.png"))
print(id2 :=system.save_file(r"C:\Users\Всеволод\Desktop\temp.bmp"))
print(system.get_file(id))
print(system.get_list([id]))
print(system.get_all())
print(system.rename_file(id,id1 :=f"{id}1"))
print(system.get_all())
print(system.flush_file(id1))
print(system.get_all())
print(system.flush_all())

