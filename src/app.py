from flask import Flask, jsonify, request
from config import config
import pandas as pd
import numpy as np
from fastavro import writer, parse_schema, reader
from flask_mysqldb import MySQL

app = Flask(__name__)

conexion = MySQL(app)

#GET
@app.route('/departments', methods=['GET'])
def get_departments():
    try:
        cursor=conexion.connection.cursor()
        sql="SELECT * FROM departments"
        cursor.execute(sql)
        data=cursor.fetchall()
        departments=[]
        for row in data:
            department = {'id':row[0], 'departament':row[1]}
            departments.append(department)
        return jsonify({'departaments':departments, 'message':"list of departaments"})
    except Exception as ex:
        return jsonify({'message': "Error"})

@app.route('/jobs', methods=['GET'])
def get_jobs():
    try:
        cursor=conexion.connection.cursor()
        sql="SELECT * FROM jobs"
        cursor.execute(sql)
        data=cursor.fetchall()
        jobs=[]
        for row in data:
            job = {'id':row[0], 'job':row[1]}
            jobs.append(job)
        return jsonify({'jobs':jobs, 'message':"list of jobs"})
    except Exception as ex:
        return jsonify({'message': "Error"})

@app.route('/hired_employees', methods=['GET'])
def get_hired_employees():
    try:
        cursor=conexion.connection.cursor()
        sql="SELECT * FROM hired_employees"
        cursor.execute(sql)
        data=cursor.fetchall()
        jobs=[]
        for row in data:
            job = {'id':row[0], 'name':row[1], 'datetime':row[2], 'department_id':row[3], 'job_id':row[4]}
            jobs.append(job)
        return jsonify({'jobs':jobs, 'message':"list of hired_employees"})
    except Exception as ex:
        return jsonify({'message': "Error"})

#POST
@app.route('/departments', methods=['POST'])
def append_departments():
    try:
        cursor=conexion.connection.cursor()
        sql="INSERT INTO departments (ID, DEPARTAMENT) VALUES ('{0}','{1}')".format(request.json['id'],request.json['departament'])
        cursor.execute(sql)
        conexion.connection.commit()
        return jsonify({'message': "inserted department"})
    except Exception as ex:
        return jsonify({'message': "Error"})
    

@app.route('/jobs', methods=['POST'])
def append_jobs():
    try:
        cursor=conexion.connection.cursor()
        sql="INSERT INTO jobs (ID, JOB) VALUES ('{0}','{1}')".format(request.json['id'],request.json['job'])
        cursor.execute(sql)
        conexion.connection.commit()
        return jsonify({'message': "inserted job"})
    except Exception as ex:
        return jsonify({'message': "Error"})


@app.route('/hired_employees', methods=['POST'])
def append_hired_employees():
    try:
        cursor=conexion.connection.cursor()
        sql="INSERT INTO hired_employees (ID, NAME, DATETIME, DEPARTMENT_ID, JOB_ID) VALUES ('{0}','{1}','{2}','{3}','{4}')".format(request.json['id'],request.json['name'],request.json['datetime'],request.json['department_id'],request.json['job_id'])
        cursor.execute(sql)
        conexion.connection.commit()
        return jsonify({'message': "inserted hired_employees"})
    except Exception as ex:
        return jsonify({'message': "Error"})

#DELETE
@app.route('/departments/<id>', methods=['DELETE'])
def delete_departament(id):
    try:
        cursor=conexion.connection.cursor()
        sql="DELETE FROM departments WHERE id = '{}'".format(id)
        cursor.execute(sql)
        conexion.connection.commit()
        return jsonify({'message': "delete departments"})
    except Exception as ex:
        return jsonify({'message': "Error"})


@app.route('/jobs/<id>', methods=['DELETE'])
def delete_job(id):
    try:
        cursor=conexion.connection.cursor()
        sql="DELETE FROM jobs WHERE id = '{0}'".format(id)
        cursor.execute(sql)
        conexion.connection.commit()
        return jsonify({'message': "delete jobs"})
    except Exception as ex:
        return jsonify({'message': "Error"})


@app.route('/hired_employees/<id>', methods=['DELETE'])
def delete_hired_employees(id):
    try:
        cursor=conexion.connection.cursor()
        sql="DELETE FROM hired_employees WHERE id = '{0}'".format(id)
        cursor.execute(sql)
        conexion.connection.commit()
        return jsonify({'message': "delete hired_employees"})
    except Exception as ex:
        return jsonify({'message': "Error"})

#PUT
@app.route('/departments/<id>', methods=['PUT'])
def update_departament(id):
    try:
        cursor=conexion.connection.cursor()
        sql="UPDATE departments SET department = '{0}' WHERE id = '{1}'".format(request.json['department'],id)
        cursor.execute(sql)
        conexion.connection.commit()
        return jsonify({'message': "update departament"})
    except Exception as ex:
        return jsonify({'message': "Error"})


@app.route('/jobs/<id>', methods=['PUT'])
def update_job(id):
    try:
        cursor=conexion.connection.cursor()
        sql="UPDATE jobs SET job = '{0}' WHERE id = '{1}'".format(request.json['job'],id)
        cursor.execute(sql)
        conexion.connection.commit()
        return jsonify({'message': "update job"})
    except Exception as ex:
        return jsonify({'message': "Error"})

#MIGRATION
@app.route('/historicdep/<path>', methods=['POST'])
def load_historic_departaments(path):
    try:
        file_path = "'{0}'/departments.csv".format(path)
        df_departaments = pd.read_csv(file_path, sep=',', header=None, names =['Id','Departament'])
        df_list_departament = df_departaments.to_numpy().tolist()
        cursor=conexion.connection.cursor()
        for data in df_list_departament:
            sql="INSERT INTO departments (ID, DEPARTAMENT) VALUES ('{0}','{1}')".format(data[0],data[1])
            cursor.execute(sql)
        conexion.connection.commit()
        return jsonify({'message': "inserted Departaments history data"})
    except Exception as ex:
        return jsonify({'message': "Error"})


@app.route('/historicjob/<path>', methods=['POST'])
def load_historic_jobs(path):
    try:
        file_path = "'{0}'/jobs.csv".format(path)
        df_jobs = pd.read_csv(file_path, sep=',', header=None, names =['Id','Job'])
        df_list_job = df_jobs.to_numpy().tolist()
        cursor=conexion.connection.cursor()
        for data in df_list_job:
            sql="INSERT INTO jobs (ID, DEPARTAMENT) VALUES ('{0}','{1}')".format(data[0],data[1])
            cursor.execute(sql)
        conexion.connection.commit()
        return jsonify({'message': "inserted Jobs history data"})
    except Exception as ex:
        return jsonify({'message': "Error"})
    
@app.route('/historicemp/<path>', methods=['POST'])
def load_historic_emp(path):
    try:
        file_path = "'{0}'/hired_employees.csv".format(path)
        df_employees = pd.read_csv(file_path, sep=',', header=None, names =['Id','Name','Datetime','Departament_id','Job_id'])
        df_list_emp = df_employees.to_numpy().tolist()
        cursor=conexion.connection.cursor()
        for data in df_list_emp:
            sql="INSERT INTO hired_employees (ID, NAME, DATETIME, DEPARTMENT_ID, JOB_ID) VALUES ('{0}','{1}','{2}','{3}','{4}')".format(data[0],data[1],data[2],data[3],data[4])
            cursor.execute(sql)
        conexion.connection.commit()
        return jsonify({'message': "inserted hired_employees history data"})
    except Exception as ex:
        return jsonify({'message': "Error"})

#BACKUP
@app.route('/departments/backup/<path>', methods=['POST'])
def do_backup_departments(path):
    try:
        file_path = "'{0}'/bk_departments.avro".format(path)
        cursor=conexion.connection.cursor()
        sql="SELECT * FROM departments"
        cursor.execute(sql)
        data=cursor.fetchall()
        df = pd.DataFrame(data)
        df.write.format("avro").mode("overwrite").save(file_path)
        return jsonify({'message':"Backup Departments"})
    except Exception as ex:
        return jsonify({'message': "Error"})


@app.route('/jobs/backup/<path>', methods=['POST'])
def do_backup_jobs(path):
    try:
        file_path = "'{0}'/bk_jobs.avro".format(path)
        cursor=conexion.connection.cursor()
        sql="SELECT * FROM jobs"
        cursor.execute(sql)
        data=cursor.fetchall()
        df = pd.DataFrame(data)
        df.write.format("avro").mode("overwrite").save(file_path)
        return jsonify({'message':"Backup jobs"})
    except Exception as ex:
        return jsonify({'message': "Error"})


@app.route('/hired_employees/backup/<path>', methods=['POST'])
def do_backup_hired_employees(path):
    try:
        file_path = "'{0}'/bk_hired_employees.avro".format(path)
        cursor=conexion.connection.cursor()
        sql="SELECT * FROM hired_employees"
        cursor.execute(sql)
        data=cursor.fetchall()
        df = pd.DataFrame(data)
        df.write.format("avro").mode("overwrite").save(file_path)
        return jsonify({'message':"Backup hired_employees"})
    except Exception as ex:
        return jsonify({'message': "Error"})
    

def page_no_found(error):
    return "<h1>Page not exits</h1>", 404

if __name__=='__main__':
    app.config.from_object(config['development'])
    app.register_error_handler(404,page_no_found)
    app.run()