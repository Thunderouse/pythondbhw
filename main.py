import psycopg2


def drop_all(conn):
    with conn.cursor() as cur:
        cur.execute('''
        DROP TABLE Person_phone;
        ''')
        cur.execute('''
                    DROP TABLE Phone;
                    ''')
        cur.execute('''
                    DROP TABLE Person;
                    ''')
        conn.commit()

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS Person (
            id serial PRIMARY KEY,
            first_name varchar(20) NOT NULL,
            last_name varchar(50) NOT NULL,
            email varchar(50) UNIQUE NOT NULL
        );
        ''')
        cur.execute('''
        CREATE TABLE IF NOT EXISTS Phone (
            id serial PRIMARY KEY,
            number varchar(25) UNIQUE NOT NULL
        );
            ''')
        cur.execute('''
        CREATE TABLE IF NOT EXISTS Person_phone (
            person_id integer REFERENCES Person(id),
            number_id integer REFERENCES Phone(id),
            CONSTRAINT PersonPhone PRIMARY KEY (person_id, number_id)
        );
            ''')
        conn.commit()

def add_person(conn, first_name, last_name, email, number=''):
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO Person(first_name, last_name, email)
        VALUES
        (%s, %s, %s);
        ''', (first_name, last_name, email))
        if number:
            cur.execute('''
            INSERT INTO Phone (number)
                VALUES
                (%s);
                ''', (number, ))
            cur.execute('''
            INSERT INTO Person_phone (person_id, number_id)
                VALUES
                ((SELECT id FROM Person WHERE email=%s), (SELECT id FROM Phone WHERE number=%s));
                ''', (email, number))
        conn.commit()

def add_phone(conn, email, number):
    with conn.cursor() as cur:
        cur.execute('''
                SELECT id FROM Person
                 WHERE email = %s;
                    ''', (email,))
        response = cur.fetchone()
        if response:
            id = response[0]
            cur.execute('''
                            INSERT INTO Phone (number)
                                VALUES
                                (%s);
                                ''', (number,))
            cur.execute('''
                    INSERT INTO Person_phone (person_id, number_id)
                        VALUES
                        (%s, (SELECT id FROM Phone WHERE number=%s));
                        ''', (id, number))
            conn.commit()
        else:
            print('Нет такого клиента')

def update(conn, email, old_data, new_data):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM Person LIMIT 0;")
        colnames = [desc[0] for desc in cur.description]
        cur.execute('''
                SELECT Person.id, Person.first_name, Person.last_name,
                       Person.email, Phone.id, Phone.number
                  FROM Person
                  JOIN Person_phone ON Person.id = Person_phone.person_id
                  JOIN Phone ON Phone.id = Person_phone.number_id
                 WHERE Person.email = %s;
                ''', (email,))
        all_data = cur.fetchall()[0]
        table = zip(colnames[1:], all_data[1:-2])
        if old_data[0] == '+' and old_data[1:].isdigit() \
                              and old_data in all_data \
                              and new_data[0] == '+' and new_data[1:].isdigit():
            cur.execute('''
                            UPDATE Phone
                               SET number = %s
                             WHERE id = %s;
                                ''', (new_data, all_data[4]))

        else:
            for column_name, value in table:
                if value == old_data:
                    cur.execute('''
                                    UPDATE Person
                                       SET {} = %s
                                     WHERE id = %s;
                                        '''.format(column_name), (new_data, all_data[0]))
        conn.commit()

def delete_phone(conn, number):
    with conn.cursor() as cur:
        cur.execute('''
                SELECT id 
                  FROM Phone
                 WHERE number = %s;
                    ''', (number,))
        number_id = cur.fetchone()[0]
        cur.execute('''
                DELETE FROM Person_phone
                WHERE number_id = %s;
                ''', (number_id,))
        cur.execute('''
                    DELETE FROM Phone
                    WHERE number = %s;
                    ''', (number,))
        conn.commit()

def delete_person(conn, email):
    with conn.cursor() as cur:
        cur.execute('''
                        SELECT Person.id, Phone.id
                          FROM Person
                          LEFT JOIN Person_phone ON Person.id = Person_phone.person_id
                          LEFT JOIN Phone ON Phone.id = Person_phone.number_id
                         WHERE email = %s;
                            ''', (email,))
        person_id, number_id = cur.fetchone()
        if number_id:
            cur.execute('''
                            DELETE FROM Person_phone
                            WHERE person_id = %s;
                            ''', (person_id,))
            cur.execute('''
                            DELETE FROM Phone
                            WHERE id = %s;
                            ''', (number_id,))
        cur.execute('''
                        DELETE FROM Person
                        WHERE id = %s;
                        ''', (person_id,))
        conn.commit()

def search(conn, first_name=None, last_name=None, email=None, number=None):
    if first_name:
        column_and_value = ('Person.first_name', first_name)
    elif last_name:
        column_and_value = ('Person.last_name', last_name)
    elif email:
        column_and_value = ('Person.email', email)
    elif number:
        column_and_value = ('Phone.number', number)
    else:
        print('Укажите строго 1 значение')
        return
    with conn.cursor() as cur:
        cur.execute('''
                        SELECT Person.id, Person.first_name, Person.last_name, Person.email,
                               Phone.number
                          FROM Person
                          LEFT JOIN Person_phone ON Person.id = Person_phone.person_id
                          LEFT JOIN Phone ON Phone.id = Person_phone.number_id
                         WHERE {} = %s;
                            '''.format(column_and_value[0]), (column_and_value[1],))
        response = cur.fetchall()
        if response:
            colnames = [desc[0] for desc in cur.description]
            table = [str(i) for i in tuple(zip(colnames, *response))]
            print('\n'.join(table))
        else:
            print('Клиент не найден')


with psycopg2.connect(database="", user="", password="") as conn:

    #drop_all(conn)

    create_tables(conn)
    add_person(conn, 'Ivan', 'Ivanov', 'ivan@mail.ru')
    add_person(conn, 'Ivan', 'Semenov', 'semenov@mail.ru')
    add_person(conn, 'Petr', 'Petrov', 'petrov2@mail.ru', '+79191234567')
    add_phone(conn, 'ivan@mail.ru', '+79210987654')

    update(conn, 'petrov2@mail.ru', 'Petr', 'Пётр')
    update(conn, 'petrov2@mail.ru', '+79191234567', '+7000')
    delete_phone(conn, '+7000')
    delete_person(conn, 'petrov2@mail.ru')

    search(conn, first_name='Ivan')
    search(conn, last_name='Petrov')

conn.close()
