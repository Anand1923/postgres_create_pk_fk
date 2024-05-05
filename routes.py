from fastapi import  File, UploadFile, HTTPException,APIRouter
import csv
import io
import psycopg2
import time
import yaml


with open('config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)

is_lower = True



def connect_to_db(max_retries=3, retry_interval=5):
    attempts = 0
    while attempts < max_retries:
        try:
            connection = psycopg2.connect(
                host=config_data.get('HOST'),
                database=config_data.get('DB'),
                user=config_data.get('USER'),
                password=config_data.get('PASSWORD')
            )
            print("Connection to database successful.")
            return connection
        except psycopg2.OperationalError as e:
            attempts += 1
            print(f"Error connecting to database (attempt {attempts}): {e}")
            print(f"Retrying connection after {retry_interval} seconds...")
            time.sleep(retry_interval)
    
    print("Max connection attempts reached. Unable to connect to database.")
    return None



def check_columns(csv_data):
    connection = connect_to_db()
    cursor = connection.cursor()
    print(csv_data)
    errors = []
    for table_name, column_name in csv_data:
        try:
            cursor.execute(f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name} GROUP BY {column_name};")
            count = cursor.fetchone()[0]
            if count != 0:
                errors.append(f"Non-unique values found in column '{column_name}' of table '{table_name}'.")
            
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL;")
            count = cursor.fetchone()[0]
            if count != 0:
                errors.append(f"Null values found in column '{column_name}' of table '{table_name}'.")
        except psycopg2.ProgrammingError as e:
            errors.append(f"{e}")
            # Rollback the transaction in case of an error
            connection.rollback()
        except Exception as e:
            errors.append(f"Unknown error occurred while processing table '{table_name}' and column '{column_name}': {e}")

    cursor.close()
    connection.close()

    return errors

def generate_sql_pk(primary_key_name, table_name, column_name):
    return f"ALTER TABLE {table_name} ADD CONSTRAINT {primary_key_name} PRIMARY KEY ({column_name});"

def generate_constraint_fk(constraint_name, table_name, constraint_type, column_name, referenced_table_name, referenced_column_name):
    return f"""
        ALTER TABLE {table_name}  ADD CONSTRAINT {constraint_name}
        {constraint_type} ({column_name})  REFERENCES {referenced_table_name}({referenced_column_name});
    """

def constraint_exists(cursor, constraint_name, table_name):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.table_constraints
        WHERE constraint_name = %s
        AND table_name = %s;
    """, (constraint_name, table_name))
    return cursor.fetchone()[0] > 0

router=APIRouter(prefix="/v1",tags=["Upload CSV"])



@router.post("/data_sanity_check")
async def data_sanity_check(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        csv_data = io.StringIO(contents.decode("utf-8"))

        reader = csv.reader(csv_data)
        next(reader) 
        table_column_pairs = [(row[1], row[2]) for row in reader] 

        # Check uniqueness and non-null values in columns
        errors = check_columns(table_column_pairs)
        filtered_errors = sorted([error for error in errors if any(c for c in error)])

        if errors:
            return {"message": "Errors found in database columns", "errors": errors}
        else:
            return {"message": "All columns are unique and non-null"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing CSV file: {e}")


@router.post("/create_primary_keys")
async def create_primary_keys(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        csv_data = io.StringIO(contents.decode("utf-8"))

        connection = connect_to_db()
        cursor = connection.cursor()

        reader = csv.DictReader(csv_data)
        success_list = []
        error_list = []
        for row in reader:
            primary_key_name = row['PrimaryKeyName']
            table_name = row['TableName']
            column_name = row['ColumnName']
            if is_lower:
                primary_key_name = row['PrimaryKeyName'].lower()
                table_name = row['TableName'].lower()
                column_name = row['ColumnName'].lower()
            # Check if primary key constraint already exists
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.table_constraints
                WHERE constraint_type = 'PRIMARY KEY'
                AND constraint_name = %s
                AND table_name = %s
            """, (primary_key_name, table_name))
            if cursor.fetchone()[0] == 0:
                # Primary key constraint does not exist, proceed with adding it
                sql_statement = generate_sql_pk(primary_key_name, table_name, column_name)
                try:
                    cursor.execute(sql_statement)
                    connection.commit()
                    success_list.append(f"Successfully created primary key for {table_name}.{column_name}")
                except Exception as e:
                    connection.rollback()
                    error_list.append(f"Failed to create primary key for {table_name}.{column_name}: {e}")
            else:
                # Primary key constraint already exists, skip
                success_list.append(f"Primary key {primary_key_name} already exists for table {table_name}")
        
        return {"success": success_list, "errors": error_list}
    except Exception as e:
        print(type(e).__name__, __file__, e.__traceback__.tb_lineno ,e.args)
        raise HTTPException(status_code=500, detail=f"Error creating primary keys: {e}")
    finally:
        cursor.close()
        connection.close()



@router.post("/create_foreign_keys")
async def create_foreign_keys(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        csv_data = io.StringIO(contents.decode("utf-8"))

        connection = connect_to_db()
        cursor = connection.cursor()

        reader = csv.DictReader(csv_data)
        success_list = []
        error_list = []
        for row in reader:
            constraint_name = row['ConstraintName']
            table_name = row['TableName']
            constraint_type = row['ConstraintType']
            column_name = row['ColumnName']
            referenced_table_name = row['ReferencedTableName']
            referenced_column_name = row['ReferencedColumnName']
            if is_lower:
                constraint_name = row['ConstraintName'].lower()
                table_name = row['TableName'].lower()
                constraint_type = row['ConstraintType'].lower()
                column_name = row['ColumnName'].lower()
                referenced_table_name = row['ReferencedTableName'].lower()
                referenced_column_name = row['ReferencedColumnName'].lower()

            # Check if constraint already exists
            if constraint_exists(cursor, constraint_name, table_name):
                error_list.append(f"Constraint '{constraint_name}' already exists on table '{table_name}'")
                continue

            sql_statement = generate_constraint_fk(constraint_name, table_name, constraint_type, column_name, referenced_table_name, referenced_column_name)
            try:
                print(sql_statement)
                cursor.execute(sql_statement)
                connection.commit()
                success_list.append(f"Successfully created foreign key '{constraint_name}' on '{table_name}.{column_name}' referencing '{referenced_table_name}.{referenced_column_name}'")
            except Exception as e:
                connection.rollback()
                error_list.append(f"Failed to create foreign key '{constraint_name}': {e}")

        return {"success": success_list, "errors": error_list}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating foreign keys: {e}")
    finally:
        cursor.close()
        connection.close()