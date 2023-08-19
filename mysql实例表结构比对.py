#!/usr/bin/env python3
import sys
import socket
import logging
import time
import re
import pymysql
import subprocess


from typing import Optional



def check_connection(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)  
    try:
        sock.connect((host, int(port)))
        sock.shutdown(socket.SHUT_RDWR)
        return True
    except:
        return False
    finally:
        sock.close()
		
		

print("")




host_source = input("请输入需要源端所在的实例地址:")
username_source = input("请输入源端使用的用户名:")  
password_source_pymysql = input("请输入源端使用的密码:")
port_source = input("请输入源端使用的端口:")
port_source = int(port_source)


target_host = input("请输入需要导入对象所在的实例地址:")
target_user = input("请输入导入实例使用的用户名:")
target_password_pymysql = input("请输入导入实例使用的密码:")  
target_port = input("请输入导入实例使用的端口:")
target_port = int(target_port)


compare_tablestrct = input("是否比对表结构,输入y/n:")
compare_tablestrct = compare_tablestrct.strip().lower()

compare_index = input("是否比对索引,输入y/n:")
compare_index = compare_index.strip().lower()


compare_charactor = input("是否比对字符集,输入y/n:")
compare_charactor = compare_charactor.strip().lower()

schema_a = input("请输入要比对的源库schema:")
schema_a = schema_a.strip()
if not schema_a:
    schema_a = None

compare_table = input("请输入要比对的表名称,不写默认对比指定schema下的所有表:")
compare_table = compare_table.strip()
if not compare_table:
    compare_table = None


schema_b = input("请输入要比对的目标实例schema:")
schema_b = schema_b.strip()
print("")
if not schema_b:
    schema_b = None	



def get_conn_info(instance):
    return {
        'host': instance['host'],
        'user': instance['user'],
        'port': instance['port'],
        'password': instance['password'],
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }

def get_databases(conn, specified_schema: Optional[str] = None):
    with conn.cursor() as cursor:
        if specified_schema is None:
            cursor.execute("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', '__recycle_bin__','performance_schema', 'sys')")
        else:
            cursor.execute("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s", (specified_schema,))
        result = cursor.fetchall()
    return [db['SCHEMA_NAME'] for db in result]

def get_tables(conn, database, specified_table: Optional[str] = None):
    with conn.cursor() as cursor:
        if specified_table is None:
            cursor.execute(f"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{database}'")
        else:
            cursor.execute(f"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = %s", (specified_table,))
        result = cursor.fetchall()
    return [table['TABLE_NAME'] for table in result]

def get_columns(conn, database, table):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = '{table}'")
        result = cursor.fetchall()
    return result

def compare_columns(col_a, col_b):
    ignored_fields = {'TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME'}
    return {k: v for k, v in col_a.items() if k not in ignored_fields} == {k: v for k, v in col_b.items() if k not in ignored_fields}

def generate_create_database_sql(schema):
    return f"CREATE DATABASE `{schema}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

def get_create_table_sql(conn, database, table):
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW CREATE TABLE `{database}`.`{table}`")
        result = cursor.fetchone()
    return result['Create Table']

def generate_alter_table_sql(schema_a, table_a, columns_a, columns_b):
    sql = []
    for column_a in columns_a:
        column_b = next((col for col in columns_b if col['COLUMN_NAME'] == column_a['COLUMN_NAME']), None)
        if column_b is None:
            col_def = f"{column_a['COLUMN_NAME']} {column_a['COLUMN_TYPE']}"
            if column_a['IS_NULLABLE'] == 'NO':
                col_def += " NOT NULL"
            if column_a['COLUMN_DEFAULT'] is not None:
                col_def += f" DEFAULT {column_a['COLUMN_DEFAULT']}"
            if column_a['EXTRA']:
                col_def += f" {column_a['EXTRA']}"
            sql.append(f"ALTER TABLE `{schema_a}`.`{table_a}` ADD COLUMN {col_def};")
        elif not compare_columns(column_a, column_b):
            col_def = f"{column_a['COLUMN_NAME']} {column_a['COLUMN_TYPE']}"
            if column_a['IS_NULLABLE'] == 'NO':
                col_def += " NOT NULL"
            if column_a['COLUMN_DEFAULT'] is not None:
                col_def += f" DEFAULT {column_a['COLUMN_DEFAULT']}"
            if column_a['EXTRA']:
                col_def += f" {column_a['EXTRA']}"
            sql.append(f"ALTER TABLE `{schema_a}`.`{table_a}` MODIFY COLUMN {col_def};")
    return sql

def get_indices(conn, database, table):
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW INDEX FROM `{database}`.`{table}`")
        result = cursor.fetchall()
    
    
    indices = {}
    for row in result:
        if row['Key_name'] not in indices:
            indices[row['Key_name']] = dict(row)
            indices[row['Key_name']]['Column_name'] = [row['Column_name']]
        else:
            indices[row['Key_name']]['Column_name'].append(row['Column_name'])
    
    return list(indices.values())

def generate_create_index_sql(idx):
    index_type = "UNIQUE" if idx["Non_unique"] == 0 else ""
    columns = ', '.join(f'`{col}`' for col in idx['Column_name'])
    return f"ALTER TABLE `{idx['Table']}` ADD {index_type} INDEX `{idx['Key_name']}` ({columns});"




def compare_indices(idx_a, idx_b):
    ignored_fields = {'Table', 'Seq_in_index', 'Cardinality', 'Sub_part', 'Packed', 'Null', 'Index_comment'}
    idx_a_filtered = {k: v for k, v in idx_a.items() if k not in ignored_fields}
    idx_b_filtered = {k: v for k, v in idx_b.items() if k not in ignored_fields}

    if idx_a_filtered.keys() != idx_b_filtered.keys():
        return False

    for key in idx_a_filtered.keys():
        if isinstance(idx_a_filtered[key], list):
            if sorted(idx_a_filtered[key]) != sorted(idx_b_filtered[key]):
                return False
        elif idx_a_filtered[key] != idx_b_filtered[key]:
            return False

    return True


def generate_drop_index_sql(idx):
    return f"ALTER TABLE `{idx['Table']}` DROP INDEX `{idx['Key_name']}`;"

def get_table_character_set_and_collation(conn, database, table):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT CCSA.character_set_name, CCSA.collation_name FROM information_schema.`TABLES` T, information_schema.`COLLATION_CHARACTER_SET_APPLICABILITY` CCSA WHERE CCSA.collation_name = T.table_collation AND T.table_schema = '{database}' AND T.table_name = '{table}'")
        result = cursor.fetchone()
    return result

def generate_alter_table_character_set_and_collation_sql(database, table, character_set, collation):
    return f"ALTER TABLE `{database}`.`{table}` CONVERT TO CHARACTER SET {character_set} COLLATE {collation};"
	
	

def main(instance_a, instance_b, schema_a=None, schema_b=None, table=None):
    
   
    conn_a = pymysql.connect(**get_conn_info(instance_a)) 
    conn_b = pymysql.connect(**get_conn_info(instance_b))
    

    mark=schema_b
    databases_a = get_databases(conn_a, schema_a)
    databases_b = get_databases(conn_b, schema_b)

    if not databases_b:
        print("")
        print(f" -- 目标实例输入的schema:{schema_b}在实例:{compare_b_mark}中不存在,退出本次比对！")
        sys.exit()

    for schema_a, schema_b in zip(databases_a, databases_b):
        tables_a = get_tables(conn_a, schema_a, table)
        tables_b = get_tables(conn_b, schema_b, table)

        if schema_a not in databases_b  and mark is  None:
            
            create_database_sql = generate_create_database_sql(schema_a)
            print(create_database_sql)
           

        for table_a in tables_a:
            if table_a not in tables_b :
                create_table_sql = get_create_table_sql(conn_a, schema_a, table_a)
                #print(f"\n -- 创建的表名称为: `{schema_b}`.`{table_a}`:")
                if compare_tablestrct == 'y':
                    print(f"use `{schema_b}`;")
                    print(f"{create_table_sql};")
               
            else:
                columns_a = get_columns(conn_a, schema_a, table_a)
                columns_b = get_columns(conn_b, schema_b, table_a)

                alter_statements = generate_alter_table_sql(schema_b, table_a, columns_a, columns_b)

                if  alter_statements:
               
                    for statement in alter_statements:
                        #print(f"use `{schema_b}`;")
                       if compare_tablestrct == 'y':
                          print(statement)
              
                if compare_index == 'y':
                     indices_a = get_indices(conn_a, schema_a, table_a)
                     indices_b = get_indices(conn_b, schema_b, table_a)
			       
                     for idx_a in indices_a:
                         idx_b = next((idx for idx in indices_b if idx['Key_name'] == idx_a['Key_name']), None)
                         if idx_b is None:
                             sql = generate_create_index_sql(idx_a)
                             #print(f"\n -- 为表名称为 `{schema_b}`.`{table_a}`的表添加索引 `{idx_a['Key_name']}` :")
                             print(f"use `{schema_b}`;")
                    
                             print(sql)
                   
                         elif not compare_indices(idx_a, idx_b):
                             sql = generate_drop_index_sql(idx_b) + "\n" + generate_create_index_sql(idx_a)
                             #print(f"\n -- 索引信息不一致，需要为表 `{schema_b}`.`{table_a}` 先删除后创建 `{idx_a['Key_name']}`索引:")
                             print(f"use `{schema_b}`;")
                             print(sql)
                   
			       
                    
                     for idx_b in indices_b:
                         idx_a = next((idx for idx in indices_a if idx['Key_name'] == idx_b['Key_name']), None)
                         if idx_a is None:
                             sql = generate_drop_index_sql(idx_b)
                             print(f"\n -- 由于目标对象的索引比源库索引多，为保持绝对一致,需要删除表名称为:`{schema_b}`.`{table_a}` 索引名称为`{idx_b['Key_name']}` 的索引，删除可能影响目标实例性能，需要谨慎执行:")
                             print(f"use `{schema_b}`;")
                             print(sql)
                else :
                     print("")
				
                if  compare_charactor == 'y':				
                     charset_collation_a = get_table_character_set_and_collation(conn_a, schema_a, table_a)
                     charset_collation_b = get_table_character_set_and_collation(conn_b, schema_b, table_a)
                                     
                     if not charset_collation_b:
                         continue
			       
                     if charset_collation_a != charset_collation_b:
                         if 'CHARACTER_SET_NAME' in charset_collation_a and 'COLLATION_NAME' in charset_collation_a:
                             sql = generate_alter_table_character_set_and_collation_sql(schema_b, table_a, charset_collation_a['CHARACTER_SET_NAME'], charset_collation_a['COLLATION_NAME'])
                            
                             print(sql)
                         else:
                             print(f"\n -- 不能正确获取`{schema_b}`.`{table_a}`的字符集和排序信息 ")
                else:
                     print("")	

 
    conn_a.close()
    conn_b.close()

if __name__ == "__main__":

    # 构造手动获取的连接信息
    instance_a = {
        'host': host_source,
        'user': username_source,
        'port': port_source,
        'password': password_source_pymysql
    }

    instance_b = {
        'host': target_host, 
        'user': target_user,
        'port': target_port,
        'password': target_password_pymysql
    }

    main(instance_a,instance_b, schema_a, schema_b, compare_table)