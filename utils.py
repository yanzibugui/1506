#!/usr/bin/env python
#coding=utf-8
#Created:2017/9/7
#Author:1506

import pymysql
import time
import re

def get_connect(schema):
    conn = {
        'host': 'host',
        'port': 3306,
        'user':'user',
        'password':'password',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor,
    }
    schemas = ['schema1','schema2']
    if schema not in schemas:
        print("Incorrect database of {0}@{1}".format(conn['user'], conn['host']))
        return None
    else:
        return pymysql.connect(db=schema, host=conn['host'], port=conn['port'], user=conn['user'],
                               password=conn['password'], charset=conn['charset'], cursorclass=conn['cursorclass'])

def ensure_list(obj)->list:
    if isinstance(obj,list):
        return obj
    else:
        obj_list=[]
        obj_list.append(obj)
        return obj_list

def get_sql_type(sql:str)->str:
    stype={
        'select': 'DQL',
        'create': 'DDL',
        'drop': 'DDL',
        'truncate': 'DDL',
        'alter': 'DDL',
        'update': 'DML',
        'insert': 'DML',
        'delete': 'DML',
        'grant': 'DCL',
        'rollback': 'DCL',
        'commit': 'DCL',
        'revoke': 'DCL',
    }
    return stype.get((re.match("(\s+)?(.*)",sql,re.I|re.S).group(2).split(' ')[0].lower()))

def write_log(log_body,log_tail = 'has completed!\n'):
    log=' '.join([str(time.strftime('%Y-%m-%d %H:%M:%S')), log_body, log_tail])
    print(log)
    with open(r'C:\Users\linhao02_sh\PycharmProjects\1506\etl\ccetl_job_log.txt', 'a') as ilog:
        ilog.write(log)

def execute_sql(sql_list,db='schema1')->list:
    if sql_list is None:
        print('SQL statement is null...')
        return False
    # 将sql语句格式化为list,方便迭代
    schema=get_connect(db)
    if schema is None:
        print("Database connect error...")
        return False
    else:
        try:
            with schema.cursor() as cxcn:
                sql_res=[]
                for sql in ensure_list(sql_list):
                    st=get_sql_type(sql)
                    if st is None:  # 若sql语言类型未知,则只执行
                        effect_rows=cxcn.execute(sql)
                        log=' '.join(['UPDATED',str(effect_rows),'records'])
                        write_log(log)
                        sql_res.append(True)
                    elif st=='DCL': #若为数据控制语言，则记录概要日志：直接记录sql语句
                        cxcn.execute(sql)
                        write_log(sql)
                        sql_res.append(True)
                    elif st=='DDL': #若为数据定义语言，则记录概要日志
                        cxcn.execute(sql)
                        # 匹配sql日志概要
                        g=re.match("\s*(create|drop|truncate|alter)\s+(temporary\s*)?(table|view|index)\s+(if\s*)?(not\s*)?(exists\s*)?(\w+\.)?(\w+)\s*.*",sql,re.I)
                        log=re.sub("\. ",".",' '.join([x for x in [g.group(1),g.group(2),g.group(3),g.group(7),g.group(8)] if x is not None]))
                        write_log(log)
                        sql_res.append(True)
                    elif st=='DML': # 若为数据操作语言，则记录受影响的记录数
                        effect_rows=cxcn.execute(sql)
                        schema.commit()
                        g=re.match("\s*(insert|delete|update)\s+(from|into)?\s?(\w+\.)?(\w+).*",sql,re.I)
                        log = re.sub("\. ", ".", ' '.join([x for x in [g.group(1),str(effect_rows),'records',g.group(2), g.group(3), g.group(4)] if x is not None]))
                        write_log(log)
                        sql_res.append(effect_rows)
                    elif st=='DQL': #若为数据查询语言，则记录查询结果数，并返回查询结果
                        effect_rows=cxcn.execute(sql)
                        log=' '.join(['select',str(effect_rows),'records'])
                        write_log(log)
                        res=cxcn.fetchall()
                        sql_res.append(res)
                return sql_res
        except schema.Error as e:
            print(e)
            return False
        finally:
            schema.close()

