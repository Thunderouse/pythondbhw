import psycopg2
from pprint import pprint

def create_db(conn):
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS phone_number;
        DROP TABLE IF EXISTS client;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS client (
            id_client SERIAL primary key,
            name varchar(15) not null,
            surname varchar(15) not null,
            mail varchar(255)
            );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS phone_number (
            id_phone_number SERIAL primary key,
            phone_number varchar(12),
            id_client int not null references client(id_client)
            );
    """)
    conn.commit()
    cur.close()

def add_client(conn, name=None, surname=None, mail=None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO client (name, surname, mail) 
        VALUES
            ('{0}', '{1}', '{2}');
    """.format(name, surname, mail))

    conn.commit()
    cur.close()

def add_now_client_phone(conn, find_name, *args):
    """Эта функция нужна для того,чтобы добавить записи в поля таблицы 'phone_number'"""

    cur = conn.cursor()

    cur.execute("""
        select c.id_client
        from client c
        where c.name = '{0}'
    """.format(find_name))
    try:
        find_id = cur.fetchall()[0][0]

        if len(args) >= 1:
            for phone_number in args:
                cur.execute("""
                        INSERT INTO phone_number (phone_number, id_client)
                        VALUES
                        ({0}, '{1}') RETURNING id_phone_number, phone_number, id_client
                    """.format(phone_number, find_id))
        else:
            print('Нет номера телефона')
    except:
        print('Ошибка при нахождении записи под таким индексом')

    conn.commit()
    cur.close()

def update_client(conn, id, name=None, surname=None, mail=None):
    cur = conn.cursor()

    cur.execute("""
        UPDATE client SET name='{0}', surname='{1}', mail='{2}' WHERE client.id_client = {3} ;
    """.format(name, surname, mail, id))

    conn.commit()
    cur.close()

def delete_client_phone(conn, find_phone_number):

    cur = conn.cursor()

    cur.execute("""
            select pn.id_phone_number 
            from phone_number pn 
            where pn.phone_number = '{0}'
        """.format(find_phone_number))
    try:
        find_id = cur.fetchall()[0][0]
        cur.execute("""
                DELETE
                FROM phone_number pn
                WHERE pn.id_phone_number = %s
            """, (find_id,))
        conn.commit()
        cur.close()
    except:
        print("Ошибка при нахождении записи в таблице 'phone_number', возможно неверно указано имя поля записи")

def delete_client(conn, find_client):
    cur = conn.cursor()
    try:
        cur.execute("""
                    select pn.id_phone_number
                    from phone_number pn  
                    where pn.id_client = (select c.id_client
                                          from client c
                                          where c.name = '{0}')
                """.format(find_client))
        find_id = ()
        if find_id is None:
            for number in cur.fetchall():
                find_id += number


            cur.execute("""
                delete
                from phone_number pn
                where pn.id_phone_number in {0}
                    """.format(find_id))
        cur.execute("""
            select c.id_client
            from client c
            where c.name = '{0}'
        """.format(find_client))
        find_id_client = cur.fetchall()[0][0]
        cur.execute("""
            delete
            from client c
            where c.id_client = {0}
        """.format(find_id_client))
        conn.commit()
        cur.close()
    except:
        print("Ошибка при удалении клиента в таблице 'client'")

def find_client(conn, name=None, surname=None, mail=None, phone_number=None):
    cur = conn.cursor()
    info = None
    if name is not None:
        cur.execute("""
            select c.name, c.surname, c.mail, pn.phone_number 
            from client c join phone_number pn on pn.id_client = c.id_client 
            where c.name = '{0}'
        """.format(name))
        info = cur.fetchall()
    elif surname is not None:
        cur.execute("""
            select c.name, c.surname, c.mail, pn.phone_number 
            from client c join phone_number pn on pn.id_client = c.id_client 
            where c.surname = '{0}'
        """.format(surname))
        info = cur.fetchall()
    elif mail is not None:
        cur.execute("""
            select c.name, c.surname, c.mail, pn.phone_number 
            from client c join phone_number pn on pn.id_client = c.id_client 
            where c.mail = '{0}'
        """.format(surname))
        info = cur.fetchall()
    elif phone_number is not None:
        cur.execute("""
            select c.name, c.surname, c.mail, pn.phone_number 
            from client c join phone_number pn on pn.id_client = c.id_client 
            where pn.phone_number = '{0}'
        """.format(surname))
        info = cur.fetchall()
    else:
        print('Запись не найдена')

    print(info)
    conn.commit()
    cur.close()

if __name__ == '__main__':

    with psycopg2.connect(database="python_test", user='', password='') as conn:
        create_db(conn)

        add_client(conn, 'Bob', 'Booble', 'Bob64@gmail.com')
        add_client(conn, 'Vova', 'Booble', 'Bob64@gmail.com')

        update_client(conn, 2, 'igor', 'pop', 'pop@gmail.com')

        add_now_client_phone(conn, 'igor', '89288880809', '89288881111')
        add_now_client_phone(conn, 'Bob', '89288882222')

        delete_client_phone(conn, '89288882222')
        delete_client(conn, 'Bob')

        find_client(conn)
