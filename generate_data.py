import uuid
import random
import boto3
import pandas as pd
import mysql.connector
from faker import Faker

fake = Faker()


db_connection = mysql.connector.connect(
    host='172.31.19.141',
    user='root',
    password='utec',
    database='ingesta01Py'
)
cursor = db_connection.cursor()

# Configuración S3
s3_client = boto3.client(
    's3',
    aws_access_key_id='TU_ACCESS_KEY_ID',
    aws_secret_access_key='TU_SECRET_KEY',
    aws_session_token='TU_SESSION_TOKEN'
)
BUCKET_NAME = 'mi-bucket-ingesta-s3'


def generate_sedes(n=2000):
    print("Generando sedes...")
    sedes_data = []
    for _ in range(n):
        sede = {
            'id': str(uuid.uuid4()),
            'name': fake.company(),
            'address': fake.address(),
            'phone': fake.phone_number()
        }
        sedes_data.append(sede)

    
    insert_query = """
        INSERT INTO sede (id, name, address, phone)
        VALUES (%s, %s, %s, %s)
    """
    values = [(s['id'], s['name'], s['address'], s['phone']) for s in sedes_data]
    cursor.executemany(insert_query, values)
    db_connection.commit()

  
    pd.DataFrame(sedes_data).to_csv('sedes.csv', index=False)
    upload_to_s3('sedes.csv')
    print(f"{n} sedes insertadas.")
    return [s['id'] for s in sedes_data]  # Devolver IDs



def generate_employees(sede_ids, n=20000):
    print("Generando empleados...")
    employees_data = []
    for _ in range(n):
        emp = {
            'id': str(uuid.uuid4()),
            'name': fake.first_name(),
            'last_name': fake.last_name(),
            'age': random.randint(18, 65),
            'phone': fake.phone_number(),
            'email': fake.email(),
            'salary': round(random.uniform(1500, 3500), 2),
            'role': random.choice(['Trainer', 'Nutricionist', 'Administrator']),
            'sede_id': random.choice(sede_ids)
        }
        employees_data.append(emp)

    insert_query = """
        INSERT INTO employees (id, name, last_name, age, phone, email, salary, role, sede_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = [
        (e['id'], e['name'], e['last_name'], e['age'], e['phone'],
         e['email'], e['salary'], e['role'], e['sede_id'])
        for e in employees_data
    ]
    cursor.executemany(insert_query, values)
    db_connection.commit()

    pd.DataFrame(employees_data).to_csv('employees.csv', index=False)
    upload_to_s3('employees.csv')
    print(f"{n} empleados insertados.")


# SUBIDA A S3
def upload_to_s3(file_name):
    try:
        s3_client.upload_file(file_name, BUCKET_NAME, file_name)
        print(f"Subido a S3: {file_name}")
    except Exception as e:
        print(f"Error al subir {file_name} a S3: {e}")



if __name__ == '__main__':
    sede_ids = generate_sedes()
    generate_employees(sede_ids)
    cursor.close()
    db_connection.close()
    print("Ingesta finalizada.")