import mysql.connector
from mysql.connector import errorcode

# Abrindo conexão com o BD
cnx = mysql.connector.connect(
    user='root',
    password='secret_password',
    host='localhost',
    database='DW',
    port='6033'
)

if cnx.is_connected():
    db_info = cnx.get_server_info()
    print("Connected to MySQL server version ", db_info)
    cursor = cnx.cursor()
    cursor.execute("SELECT DATABASE();")
    line = cursor.fetchone()
    print("Connected to database ", line)
else:
    cursor.close()
    cnx.close()
    print("MySQL connection has been closed")

# ----------------------------------------------------------------------------------------------------------

# Definindo as tabelas
tables = {}

tables['beers'] = (
    "CREATE TABLE IF NOT EXISTS `beers` ("
    "  `id` INT AUTO_INCREMENT PRIMARY KEY,"
    "  `name` VARCHAR(255) NOT NULL,"
    "  `style` VARCHAR(255) NOT NULL,"
    "  `brewery` VARCHAR(255) NOT NULL,"
    "  `price` FLOAT NOT NULL"
    ") ENGINE=InnoDB"
)

tables['customers'] = (
    "CREATE TABLE IF NOT EXISTS `customers` ("
    "  `cpf` VARCHAR(255)  PRIMARY KEY,"
    "  `name` VARCHAR(255) NOT NULL,"
    "  `email` VARCHAR(255) NOT NULL,"
    "  `age` INT NOT NULL,"
    "  `credit` FLOAT NOT NULL DEFAULT 0"
    ") ENGINE=InnoDB"
)

tables['taps'] = (
    "CREATE TABLE IF NOT EXISTS `taps` ("
    "  `id` INT AUTO_INCREMENT PRIMARY KEY,"
    "  `beer_id` INT NOT NULL,"
    "  FOREIGN KEY (`beer_id`) REFERENCES `beers`(`id`)"
    ") ENGINE=InnoDB"
)

tables['consumptions'] = (
    "CREATE TABLE IF NOT EXISTS `consumptions` ("
    "  `id` INT AUTO_INCREMENT PRIMARY KEY,"
    "  `customer_cpf` VARCHAR(255) NOT NULL,"
    "  `beer_id` INT NOT NULL,"
    "  `tap_id` INT NOT NULL,"
    "  `mls` INT NOT NULL,"
    "  `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
    "  FOREIGN KEY (`customer_cpf`) REFERENCES `customers`(`cpf`),"
    "  FOREIGN KEY (`beer_id`) REFERENCES `beers`(`id`),"
    "  FOREIGN KEY (`tap_id`) REFERENCES `taps`(`id`)"
    ") ENGINE=InnoDB"
)

tables['payments'] = (
    "CREATE TABLE IF NOT EXISTS `payments` ("
    "  `id` INT AUTO_INCREMENT PRIMARY KEY,"
    "  `consumption_id` INT NOT NULL,"
    "  `amount` FLOAT NOT NULL,"
    "  FOREIGN KEY (`consumption_id`) REFERENCES `consumptions`(`id`)"
    ") ENGINE=InnoDB"
)

tables['promotions'] = (
    "CREATE TABLE IF NOT EXISTS `promotions` ("
    "  `id` INT AUTO_INCREMENT PRIMARY KEY,"
    "  `beer_id` INT NOT NULL,"
    "  `new_price` FLOAT NOT NULL,"
    "  `start_time` TIMESTAMP,"
    "  `end_time` TIMESTAMP,"
    "  FOREIGN KEY (`beer_id`) REFERENCES `beers`(`id`)"
    ") ENGINE=InnoDB"
)

# ----------------------------------------------------------------------------------------------------------

# Criando as tabelas
cursor = cnx.cursor()

for table_name in tables:
    table_description = tables[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
        print("OK")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table {} already exists.".format(table_name))
        else:
            print(err.msg)

# ----------------------------------------------------------------------------------------------------------

# Fazendo as inserções
insert_beer = ("INSERT INTO beers "
               "(id, name, style, brewery, price) "
               "VALUES (%s, %s, %s, %s, %s)")

many_beer_data = [
    (1, 'Refugiada', 'Irish Red Ale', 'Nefasta', 10),
    (2, 'Fugitiva', 'Munich Helles', 'Nefasta', 10.50),
    (3, 'Exilada', 'Vienna', 'Nefasta', 12),
    (4, 'Smithwicks', 'Irish Red Ale', 'Guinness', 14)
]

try:
    cursor.executemany(insert_beer, many_beer_data)
    cnx.commit()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_DUP_ENTRY:
        print("Duplicate entry. Skipping insertion.")

# ---------------

insert_customer = ("INSERT INTO customers "
                   "(cpf, name, email, age) "
                   "VALUES (%s, %s, %s, %s)")

many_customer_data = [
    ('12345678900', 'José da Silva Santos', 'jose@gmail.com', 30),
    ('98765432100', 'Maria Martins', 'maria@gmail.com', 25),
    ('55555555555', 'Vinicius Ramos', 'vinicius@gmail.com', 40)
]

try:
    cursor.executemany(insert_customer, many_customer_data)
    cnx.commit()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_DUP_ENTRY:
        print("Duplicate entry. Skipping insertion.")

# ---------------

insert_tap = ("INSERT INTO taps "
              "(id, beer_id) "
              "VALUES (%s, %s)")

many_tap_data = [
    (1,1),
    (2,2),
    (3,3)
]

try:
    cursor.executemany(insert_tap, many_tap_data)
    cnx.commit()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_DUP_ENTRY:
        print("Duplicate entry. Skipping insertion.")

# ---------------

insert_promotion = ("INSERT INTO promotions "
                    "(id, beer_id, new_price, start_time, end_time) "
                    "VALUES (%s, %s, %s, %s, %s)")

many_promotion_data = [
    (1, 1, 8, '2023-05-24 00:00:00', '2023-05-26 23:59:59'),
    (2, 3, 9, '2023-05-25 00:00:00', '2023-05-27 23:59:59'),
    (3, 3, 8, '2023-05-28 00:00:00', '2023-05-30 23:59:59'),
]

try:
    cursor.executemany(insert_promotion, many_promotion_data)
    cnx.commit()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_DUP_ENTRY:
        print("Duplicate entry. Skipping insertion.")

# ---------------

insert_payment = ("INSERT INTO payments "
                  "(id, consumption_id, amount) "
                  "VALUES (%s, %s, %s)")

many_payment_data = [
    (1, 1, 20.0),
    (2, 2, 10.0),
    (3, 3, 15.0)
]

try:
    cursor.executemany(insert_payment, many_payment_data)
    cnx.commit()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_DUP_ENTRY:
        print("Duplicate entry. Skipping insertion.")

# ---------------

insert_consumptions = ("INSERT INTO consumptions "
                       "(id, customer_cpf, beer_id, tap_id, mls) "
                       "VALUES (%s, %s, %s, %s, %s)")

many_consumption_data = [
    (1, '12345678900', 1, 1, 500),
    (2, '98765432100', 3, 2, 300),
    (3, '55555555555', 2, 3, 200),
    (4, '55555555555', 3, 2, 150),
]

try:
    cursor.executemany(insert_consumptions, many_consumption_data)
    cnx.commit()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_DUP_ENTRY:
        print("Duplicate entry. Skipping insertion.")

# ----------------------------------------------------------------------------------------------------------

#Fazendo as consultas

# Consulta com função de agregação - Total de consumo por cliente
query = "SELECT customer_cpf, SUM(mls) AS total_consumption FROM consumptions GROUP BY customer_cpf"
cursor = cnx.cursor()
cursor.execute(query)
results = cursor.fetchall()

print()
print("--------------------------------------------")
print("Total de consumo por cliente:")
print()
for row in results:
    print("CPF do cliente: ", row[0])
    print("Total de consumo (mls): ", row[1])
    print()
print()
print("--------------------------------------------")

# Consulta com função de agregação - Contagem de cervejas por estilo e preço médio
query = "SELECT style, COUNT(*) AS beer_count, AVG(price) AS average_price FROM beers GROUP BY style"
cursor = cnx.cursor()
cursor.execute(query)
results = cursor.fetchall()
print()
print("--------------------------------------------")
print("Contagem de cervejas por estilo e preço médio:")
print()
for row in results:
    print("Estilo: ", row[0])
    print("Quantidade de cervejas: ", row[1])
    print("Preço médio: ", row[2])
    print()
print()
print("--------------------------------------------")

# Consulta com Left Join - Detalhes das cervejas e suas promoções (se houver)
query = """
SELECT beers.name, beers.price, promotions.new_price
FROM beers
LEFT JOIN promotions ON beers.id = promotions.beer_id
"""
cursor = cnx.cursor()
cursor.execute(query)
results = cursor.fetchall()
print()
print("--------------------------------------------")
print("Detalhes das cervejas e suas promoções:")
print()
for row in results:
    print("Nome da cerveja: ", row[0])
    print("Preço atual: ", row[1])
    if row[2] is not None:
        print("Preço em promoção: ", row[2])
        print()
    else:
        print("Não há promoção para esta cerveja.")
        print()
print()
print("--------------------------------------------")

# ----------------------------------------------------------------------------------------------------------

cursor.close()
cnx.close()