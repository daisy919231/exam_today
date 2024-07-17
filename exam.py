from contextlib import contextmanager
import psycopg2
from typing import Optional
from collections import namedtuple
import threading
import time
import requests
import json
# 1st problem

# dat_base = {
#     "host": "localhost",
#     "port": 5432,
#     "database": "cars",
#     "user": "postgres",
#     "password": "1111"
# }


# @contextmanager
# def connect():
#     conn = psycopg2.connect(**dat_base)
#     curr = conn.cursor()
#     try:
#         yield conn, curr
#         print('Connected!')
#     except Exception as e:
#         print(f"We had an error: {e}")
#     finally:
#         curr.close()
#         conn.close()

# with connect() as (conn, curr):
#     create_table = """
#         CREATE TABLE IF NOT EXISTS products (
#             id SERIAL PRIMARY KEY, 
#             name VARCHAR(100) NOT NULL, 
#             image VARCHAR(255) NOT NULL,  
#             price NUMERIC(20, 10) NOT NULL
#         )
#     """
    
#     curr.execute(create_table)
#     conn.commit()
#     print('Successfully committed!')




# # 2nd problem
    
# with connect() as (conn, curr):
#     query = """
#         INSERT INTO products (name, image,price)
#         VALUES (%s, %s, %s)

#     """
#     data=('apple','to be updated', 12.000)
#     curr.execute(query,data)
#     conn.commit()
#     print('Succesfully inserted')

# with connect() as (conn, curr):
#     query = """
#         update products set image=%s where id=%s

#     """
#     data=namedtuple('Updating',['image','id'])
#     data1=data('not found','1')
#     curr.execute(query,data1)
#     conn.commit()
#     print('Succesfully updated')

# with connect() as (conn, curr):
#     query = """
#         delete from  products where id=%s

#     """
#     data=namedtuple('Deleting',['id'])
#     data1=data('3')
#     curr.execute(query,data1)
#     conn.commit()
#     print('Succesfully deleted')

# with connect() as (conn, curr):
#     query = """
#         select * from products;

#     """
#     curr.execute(query)
#     rows = curr.fetchall()
#     print("\nProductss Table:")
#     for row in rows:
#         print(row)

# 4rd problem
# def print_numbers():
#     for n in range(1,6):
#         print(n)

# def print_letters():
#     for l in 'ABCDE':
#         print(l) 
#         time.sleep(1)

# thread1=threading.Thread(target=print_numbers)
# thread2=threading.Thread(target=print_letters)
# thread1.start()
# thread2.start()

# thread1.join()
# thread1.join()

# # 6th prob

# class DBConnect:
#     def __init__(self, dat_base):
#         self.dat_base = dat_base

#     def __enter__(self):
#         self.conn = psycopg2.connect(**self.dat_base)
#         self.cur = self.conn.cursor()
#         return self.conn, self.cur

#     def __exit__(self, exc_type, exc_val, exc_tb):
#         if self.conn:
#             self.conn.close()

#         if self.cur:
#             self.cur.close()

# dat_base = {
#     "host": "localhost",
#     "port": 5432,
#     "database": "cars",
#     "user": "postgres",
#     "password": "1111"
# }

# # 5th problem
# import psycopg2

# class Products:
#     def __init__(self, name, description):
#         self.name=name
#         self.description = description
#         self.conn = None

#     @classmethod
#     def connect(cls):
#         try:
#             conn = psycopg2.connect(database="homework", user="postgres", password="1111")
#             print('Connected to the PostgreSQL server.')
#             return conn
#         except (psycopg2.DatabaseError, Exception) as error:
#             print(error)

#     def create(self):
#         command = """
#         CREATE TABLE IF NOT EXISTS products(
#         product_id SERIAL PRIMARY KEY,
#         product_name VARCHAR(100) NOT NULL,
#         product_description VARCHAR(100) NOT NULL)
#         """
      
#         try:
#             with self.conn.cursor() as cur:
#                 cur.execute(command)
#                 self.conn.commit()
#                 print('Table created successfully!')
#         except (psycopg2.DatabaseError, Exception) as error:
#             print(error)

#     def save(self):
#         command = "INSERT INTO products (product_name, product_description) VALUES (%s, %s)"
#         data = (self.name, self.description)
        
#         try:
#             with self.conn.cursor() as cur:
#                 cur.execute(command, data)
#                 self.conn.commit()
#                 print('Data inserted successfully!')
#         except (psycopg2.DatabaseError, Exception) as error:
#             print(error)

# product = Products("Sample Product", "A sample product description")
# product.conn = product.connect()
# product.create()
# product.save()

# 7th problem
product_url = requests.get('https://dummyjson.com/products')
product_list = product_url.json()['products']

dat_base = {
    "host": "localhost",
    "port": 5432,
    "database": "exams",
    "user": "postgres",
    "password": "1111"
}


@contextmanager
def connect():
    conn = psycopg2.connect(**dat_base)
    curr = conn.cursor()
    try:
        yield conn, curr
        print('Connected!')
    except Exception as e:
        print(f"We had an error: {e}")
    finally:
        curr.close()
        conn.close()


with connect() as (conn, curr):
    create_table = """
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY, 
            title VARCHAR(100) NOT NULL, 
            description VARCHAR(255) NOT NULL, 
            category VARCHAR(100) NOT NULL, 
            price NUMERIC(20, 10) NOT NULL, 
            discountPercentage NUMERIC(20, 10) NOT NULL, 
            rating NUMERIC(20, 10) NOT NULL, 
            stock INT NOT NULL, 
            properties JSONB, 
            brand TEXT DEFAULT 'no brand name', 
            sku TEXT NOT NULL, 
            weight NUMERIC(20, 10) NOT NULL
        )
    """
    curr.execute(create_table)
    conn.commit()
    print('Successfully committed!')

with connect() as (conn, curr):
    query = """
        INSERT INTO products (title, description, category, price, discountPercentage, rating, stock, properties, brand, sku, weight)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for product in product_list:
        brand = product.get('brand', 'no brand name')
        curr.execute(query, (
            product['title'], product['description'], product['category'], product['price'],
            product['discountPercentage'], product['rating'], product['stock'], json.dumps(product['tags']),
            brand, product['sku'], product['weight']
        ))
    conn.commit()
    print('Successfully inserted!')