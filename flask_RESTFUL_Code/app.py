from flask import Flask
from flask import request, jsonify,Response
from flask import render_template
from hadoopCommand import run_cmd
from map_chain import userszipcode
import requests
import os
from bs4 import BeautifulSoup
def createApp():
    app = Flask(__name__)
    
    @app.route('/checkServerStatus')
    def checkServer():
        return "Server Up and Running"
    
    @app.route('/inputString', methods = ['POST'])
    def parseString():
        invalidQuery = False
        body_response = {
            'process_name':"",
            'hadoop_total_elapsed_time':"",
            'hadoop_average_map_time':"",
            'hadoop_average_merge_time':"",
            'hadoop_average_shuffle_time':"",
            'hadoop_average_reduce_time':"",
            'hadoop_query_result':"",
            'spark_time_execution':"",
            'spark_query_result':""
        }
        respData = ""
        condition_column = ""
        condition = ""
        join_table = "hadoop.join."
        join_table_spark = ""
        combination_tables = ""
        inputStr = request.json['query']
        lower_inStr = inputStr.lower()
        if("join" in lower_inStr):
            strArr = lower_inStr.split(" ")
            result = ""
            result1 = ""
            table1 = strArr[3]
            table2 = strArr[6]
            table1 = table1.title()
            table2 = table2.title()
            if(table1[0] > table2[0]):
                join_table = join_table + table2+table1
               #join_table_spark = table2 +table1
                combination_tables = table2+table1
            else:
                join_table = join_table + table1 + table2
                #join_table_spark = table1 + table2
                combination_tables = table1 + table2
            if("where" in lower_inStr):
                condition_column = strArr[10]
                condition = strArr[12]
                args_list = ['hadoop', 'jar', 'join.jar', join_table, condition_column, condition]
                result = run_cmd(args_list)
            else:
                args_list = ['hadoop', 'jar', 'join.jar', join_table]
                result = run_cmd(args_list)
            if(int(result)==0):
                if(os.path.exists("part-r-00000")):
                    os.remove("part-r-00000")
                combination_tables = combination_tables.lower()
                path_hdfs = '/cloud/output/' + combination_tables + '/part-r-00000'
                path_local = '/home/HDUSER/Desktop/flask_app/cloud/'
                args_list_2 = ['hadoop','fs','-copyToLocal', path_hdfs, path_local]
                result1 = run_cmd(args_list_2)
            if(int(result1) == 0):
                arr = []
                with open('part-r-00000') as output_file:
                    for lines in output_file:
                        lines = lines.replace("\t",",")
                        lines = lines.replace('"','')
                        arr.append(lines.strip())
                    body_response['hadoop_query_result'] = arr
            path = "/usr/local/hadoop/logs/userlogs"
            all_subdirs = [d for d in os.listdir(path)]
            all_subdirs.sort(reverse = True)
            process = all_subdirs[0]
            #print(all_subdirs)
            body_response['process_name'] = process
            job = "job_"+process[12:]
            url = "http://localhost:19888/jobhistory/job/"+job
            r = requests.get(url)
            soup = BeautifulSoup(r.content, 'html5lib')
            rows_odd = soup.find_all('tr', class_ = "odd")
            rows_odd = rows_odd[:-3]
            arr_td = [td.text for td in rows_odd]
            total_elapsed_time = arr_td[4]
            total_elapsed_time = total_elapsed_time.replace("\n","")
            total_elapsed_time = total_elapsed_time.replace(" ","")
            total_elapsed_time = total_elapsed_time[8:]
            body_response['hadoop_total_elapsed_time'] = total_elapsed_time
            average_map_time = arr_td[5]
            average_map_time = average_map_time.replace("\n","")
            average_map_time = average_map_time.replace(" ","")
            average_map_time = average_map_time[14:]
            body_response['hadoop_average_map_time'] = average_map_time
            average_merge_time = arr_td[6]
            average_merge_time = average_merge_time.replace("\n","")
            average_merge_time = average_merge_time.replace(" ","")
            average_merge_time = average_merge_time[-4:]
            body_response['hadoop_average_merge_time'] = average_merge_time

            rows_even = soup.find_all('tr', class_ = "even")
            rows_even = rows_even[4:]
            arr_td_even = [td.text for td in rows_even]
            average_shuffle_time = arr_td_even[1]
            average_shuffle_time = average_shuffle_time.replace("\n","")
            average_shuffle_time = average_shuffle_time.replace(" ","")
            average_shuffle_time = average_shuffle_time[-4:]
            body_response['hadoop_average_shuffle_time'] = average_shuffle_time
            average_reduce_time = arr_td_even[2]
            average_reduce_time = average_reduce_time.replace("\n","")
            average_reduce_time = average_reduce_time.replace(" ","")
            average_reduce_time = average_reduce_time[-4:]
            body_response['hadoop_average_reduce_time'] = average_reduce_time

            #spark code
            if("where" in lower_inStr):
                condition_column = strArr[10]
                condition = strArr[12]
                join_table_spark = combination_tables.lower() + ".py"
                args_list = ['python3', join_table_spark, condition_column, condition]
                result = run_cmd(args_list)
            else:
                join_table_spark = combination_tables.lower() + ".py"
                args_list = ['python3', join_table_spark]
                result = run_cmd(args_list)
            if(int(result)==0):
                if(os.path.exists("part-00000")):
                    os.remove("part-00000")
                combination_tables = combination_tables.lower()
                path_hdfs = '/cloud/output/spark/' + combination_tables + '/part-00000'
                path_local = '/home/HDUSER/Desktop/flask_app/cloud/'
                args_list_2 = ['hadoop','fs','-copyToLocal', path_hdfs, path_local]
                result1 = run_cmd(args_list_2)
            if(int(result1) == 0):
                arr = []
                with open('part-00000') as output_file:
                    for lines in output_file:
                        lines = lines.replace("\t",",")
                        lines = lines.replace('"','')
                        arr.append(lines.strip())
                    final_arr = []
                    for x in arr:
                        x = x.replace("(","")
                        x = x.replace("'","")
                        x = x.replace(" ","")
                        x = x.replace(")","")
                        x_arr = x.split(",")
                        str2 = ""
                        user_len = 5
                        zipcode_len = 4
                        movie_len = 22
                        rating_len = 4
                        first_letter = combination_tables[0]
                        if(first_letter == "u"):
                            for i in range(1, user_len,1):
                                str2 = str2 + x_arr[i]+","
                            for i in range(user_len+1, len(x_arr), 1):
                                str2 = str2 + x_arr[i] +","
                            str2 = str2[:-1]
                            final_arr.append(str2)
                        if(first_letter == "r"):
                            for i in range(1, rating_len+1,1):
                                str2 = str2 + x_arr[i]+","
                            for i in range(rating_len+2, len(x_arr), 1):
                                str2 = str2 + x_arr[i] +","
                            str2 = str2[:-1]
                            final_arr.append(str2)
                        if(first_letter == "m"):
                            for i in range(1, rating_len+1,1):
                                str2 = str2 + x_arr[i]+","
                            for i in range(rating_len+2, len(x_arr), 1):
                                str2 = str2 + x_arr[i] +","
                            str2 = str2[:-1]
                            final_arr.append(str2)
                        

                        #print(x)
                        #x_arr = x.split(",")
                        #str2 =""
                        #length = len(group_by_columns_arr)
                        #for i in range(length, len(x_arr), 1):
                        #    str2 = str2 + x_arr[i] + ","
                        #    str2 = str2[:-1]
                        #    final_arr.append(str2)
                                #print(x_arr)    
                    body_response['spark_query_result'] = final_arr
            execution_time_spark = ""
            with open('extime.txt') as f:
                for lines in f:
                    execution_time_spark = lines
            body_response["spark_time_execution"] = execution_time_spark
            map_chain_operations = userszipcode()
            print(map_chain_operations)
            body_response["map_chain_operations"] = map_chain_operations["Map_Reduce_Operations_Chain"]
            respData = body_response
        if("group" in lower_inStr):
            join_table_2 = "hadoop.functions."
            join_table_spark = ""
            if("max" in lower_inStr):
                if("users" or "rating" in lower_inStr):
                    result = ""
                    result1 = ""
                    strArr = lower_inStr.split(" ")
                    users_table_columns = ["userid", "age", "gender", "occupation", "zipcode"]
                    rating_table_columns = ["userid", "movieid", "rating", "timestamp"]
                    selected_columns = strArr[1]
                    selected_columns = selected_columns[:-1]
                    selected_columns_arr = selected_columns.split(",")
                    table_name = strArr[4]
                    table_name = table_name.title()
                    fun_column = strArr[2]
                    fun_column = fun_column[4:-1]
                    group_by_columns = strArr[7]
                    group_by_columns_arr = group_by_columns.split(",")
                    having_condition_1 = strArr[9]
                    having_condition = strArr[11]
                    having_cond_fun_col = having_condition_1[4:-1]
                    #print(having_cond_fun_col, len(having_cond_fun_col))
                    #print(having_condition_1)
                    for s in group_by_columns_arr:
                        if table_name =="Users" and s not in users_table_columns:
                            print("failing due to users table name")
                            invalidQuery = True
                            return jsonify({'invalidQuery':"failing due to users table name in group by clause"})
                    for s in group_by_columns_arr:
                        if table_name =="Rating" and s not in users_table_columns:
                            print("failing due to Rating table name")
                            invalidQuery = True
                            return jsonify({'invalidQuery':"failing due to Rating table name in group by clause"})
                    if(selected_columns != group_by_columns):
                        print("failing due to mismatch between select columns and groupby columns")
                        invalidQuery = True
                        return jsonify({'invalidQuery':"failing due to mismatch between select columns and groupby columns"})
                    if(fun_column == "age" or fun_column == "rating"):
                        invalidQuery = False
                        #return jsonify({'invalidQuery':"This is an Invalid Query"})
                    else:
                        print(fun_column)
                        print("failing due to inappropriate column in function")
                        invalidQuery = True
                        return jsonify({'invalidQuery':"failing due to inappropriate column in function"})
                    if(having_cond_fun_col != fun_column):
                        invalidQuery = True
                        return jsonify({'invalidQuery':"failing due to mismatch in column having condition function and selection function"})
                    if(invalidQuery == False):
                        join_table_2 = join_table_2+"MaximumGroupByMain"+table_name
                        args_list = ['hadoop', 'jar', 'groupBy.jar', join_table_2, selected_columns, fun_column, group_by_columns, having_condition]
                        result = run_cmd(args_list)
                    if(int(result)==0):
                        if(os.path.exists("part-r-00000")):
                            os.remove("part-r-00000")
                        table_name = table_name.lower()
                        path_hdfs = '/cloud/output/' + table_name + '/part-r-00000'
                        path_local = '/home/HDUSER/Desktop/flask_app/cloud/'
                        args_list_2 = ['hadoop','fs','-copyToLocal', path_hdfs, path_local]
                        result1 = run_cmd(args_list_2)
                        #print(result1)
                    if(int(result1) == 0):
                        arr = []
                        with open('part-r-00000') as output_file:
                            for lines in output_file:
                                lines = lines.replace("\t",",")
                                lines = lines.replace('"','')
                                lines = lines.strip()
                                #lines = lines[3:]
                                arr.append(lines)
                            final_arr = []
                            for x in arr:
                                x_arr = x.split(",")
                                str2 =""
                                length = len(group_by_columns_arr)
                                for i in range(length, len(x_arr), 1):
                                    str2 = str2 + x_arr[i] + ","
                                str2 = str2[:-1]
                                final_arr.append(str2)
                                #print(x_arr)
                            body_response['hadoop_query_result'] = final_arr
                        path = "/usr/local/hadoop/logs/userlogs"
                        all_subdirs = [d for d in os.listdir(path)]
                        all_subdirs.sort(reverse = True)
                        process = all_subdirs[0]
                        #print(all_subdirs)
                        body_response['process_name'] = process
                        job = "job_"+process[12:]
                        url = "http://localhost:19888/jobhistory/job/"+job
                        r = requests.get(url)
                        soup = BeautifulSoup(r.content, 'html5lib')
                        rows_odd = soup.find_all('tr', class_ = "odd")
                        rows_odd = rows_odd[:-3]
                        arr_td = [td.text for td in rows_odd]
                        total_elapsed_time = arr_td[4]
                        total_elapsed_time = total_elapsed_time.replace("\n","")
                        total_elapsed_time = total_elapsed_time.replace(" ","")
                        total_elapsed_time = total_elapsed_time[8:]
                        body_response['hadoop_total_elapsed_time'] = total_elapsed_time
                        average_map_time = arr_td[5]
                        average_map_time = average_map_time.replace("\n","")
                        average_map_time = average_map_time.replace(" ","")
                        average_map_time = average_map_time[14:]
                        body_response['hadoop_average_map_time'] = average_map_time
                        average_merge_time = arr_td[6]
                        average_merge_time = average_merge_time.replace("\n","")
                        average_merge_time = average_merge_time.replace(" ","")
                        average_merge_time = average_merge_time[-4:]
                        body_response['hadoop_average_merge_time'] = average_merge_time

                        rows_even = soup.find_all('tr', class_ = "even")
                        rows_even = rows_even[4:]
                        arr_td_even = [td.text for td in rows_even]
                        average_shuffle_time = arr_td_even[1]
                        average_shuffle_time = average_shuffle_time.replace("\n","")
                        average_shuffle_time = average_shuffle_time.replace(" ","")
                        average_shuffle_time = average_shuffle_time[-4:]
                        body_response['hadoop_average_shuffle_time'] = average_shuffle_time
                        average_reduce_time = arr_td_even[2]
                        average_reduce_time = average_reduce_time.replace("\n","")
                        average_reduce_time = average_reduce_time.replace(" ","")
                        average_reduce_time = average_reduce_time[-4:]
                        body_response['hadoop_average_reduce_time'] = average_reduce_time

                        #spark code
                        table_name_spark = table_name.lower()
                        columns_spark = selected_columns
                        function_spark = "max"
                        conditon_spark = fun_column
                        having_conditon_spark = having_condition
                        join_table_spark_1 = table_name_spark +".py"
                        args_list_spark = ['python3', join_table_spark_1, columns_spark, function_spark, conditon_spark, having_conditon_spark]
                        result2 = run_cmd(args_list_spark)
                        #print("i am here")
                        if(int(result2)==0):
                            if(os.path.exists("part-00000")):
                                os.remove("part-00000")
                            #combination_tables = combination_tables.lower()
                            path_hdfs = '/cloud/output/spark/' + table_name_spark + '/part-00000'
                            path_local = '/home/HDUSER/Desktop/flask_app/cloud/'
                            args_list_4 = ['hadoop','fs','-copyToLocal', path_hdfs, path_local]
                            result3 = run_cmd(args_list_4)
                        if(int(result3) == 0):
                            arr = []
                            with open('part-00000') as output_file:
                                for lines in output_file:
                                    lines = lines.replace("\t",",")
                                    lines = lines.replace('"','')
                                    lines = lines.replace("(","")
                                    lines = lines.replace(")","")
                                    lines = lines.replace("'","")
                                    arr.append(lines.strip())
                                body_response['spark_query_result'] = arr
                        execution_time_spark = ""
                        with open('extime.txt') as f:
                            for lines in f:
                                execution_time_spark = lines
                        body_response["spark_time_execution"] = execution_time_spark
                        respData = body_response
            if("min" in lower_inStr):
                if("users" or "rating" in lower_inStr):
                    result = ""
                    result1 = ""
                    strArr = lower_inStr.split(" ")
                    users_table_columns = ["userid", "age", "gender", "occupation", "zipcode"]
                    rating_table_columns = ["userid", "movieid", "rating", "timestamp"]
                    selected_columns = strArr[1]
                    selected_columns = selected_columns[:-1]
                    selected_columns_arr = selected_columns.split(",")
                    table_name = strArr[4]
                    table_name = table_name.title()
                    fun_column = strArr[2]
                    fun_column = fun_column[4:-1]
                    group_by_columns = strArr[7]
                    group_by_columns_arr = group_by_columns.split(",")
                    having_condition_1 = strArr[9]
                    having_condition = strArr[11]
                    having_cond_fun_col = having_condition_1[4:-1]
                    #print(having_cond_fun_col, len(having_cond_fun_col))
                    #print(having_condition_1)
                    for s in group_by_columns_arr:
                        if table_name =="Users" and s not in users_table_columns:
                            print("failing due to users table name")
                            invalidQuery = True
                            return jsonify({'invalidQuery':"failing due to users table name in group by clause"})
                    for s in group_by_columns_arr:
                        if table_name =="Rating" and s not in users_table_columns:
                            print("failing due to Rating table name")
                            invalidQuery = True
                            return jsonify({'invalidQuery':"failing due to Rating table name in group by clause"})
                    if(selected_columns != group_by_columns):
                        print("selected_columns",selected_columns)
                        print("group_by_columns", group_by_columns)
                        invalidQuery = True
                        return jsonify({'invalidQuery':"failing due to mismatch between select columns and groupby columns"})
                    if(fun_column == "age" or fun_column == "rating"):
                        invalidQuery = False
                        #return jsonify({'invalidQuery':"This is an Invalid Query"})
                    else:
                        print(fun_column)
                        print("failing due to inappropriate column in function")
                        invalidQuery = True
                        return jsonify({'invalidQuery':"failing due to inappropriate column in function"})
                    if(having_cond_fun_col != fun_column):
                        invalidQuery = True
                        return jsonify({'invalidQuery':"failing due to mismatch in column having condition function and selection function"})
                    if(invalidQuery == False):
                        join_table_2 = join_table_2+"MinimumGroupByMain"+table_name
                        args_list = ['hadoop', 'jar', 'groupBy.jar', join_table_2, selected_columns, fun_column, group_by_columns, having_condition]
                        result = run_cmd(args_list)
                    if(int(result)==0):
                        if(os.path.exists("part-r-00000")):
                            os.remove("part-r-00000")
                        table_name = table_name.lower()
                        path_hdfs = '/cloud/output/' + table_name + '/part-r-00000'
                        path_local = '/home/HDUSER/Desktop/flask_app/cloud/'
                        args_list_2 = ['hadoop','fs','-copyToLocal', path_hdfs, path_local]
                        result1 = run_cmd(args_list_2)
                        #print(result1)
                    if(int(result1) == 0):
                        arr = []
                        with open('part-r-00000') as output_file:
                            for lines in output_file:
                                lines = lines.replace("\t",",")
                                lines = lines.replace('"','')
                                lines = lines.replace("(","")
                                lines = lines.replace(")","")
                                lines = lines.replace("'","")
                                lines = lines.strip()
                                #lines = lines[3:]
                                arr.append(lines)
                            final_arr = []
                            for x in arr:
                                x_arr = x.split(",")
                                str2 =""
                                length = len(group_by_columns_arr)
                                for i in range(length, len(x_arr), 1):
                                    str2 = str2 + x_arr[i] + ","
                                str2 = str2[:-1]
                                final_arr.append(str2)
                                #print(x_arr)
                            body_response['hadoop_query_result'] = final_arr
                        path = "/usr/local/hadoop/logs/userlogs"
                        all_subdirs = [d for d in os.listdir(path)]
                        all_subdirs.sort(reverse = True)
                        process = all_subdirs[0]
                        #print(all_subdirs)
                        body_response['process_name'] = process
                        job = "job_"+process[12:]
                        url = "http://localhost:19888/jobhistory/job/"+job
                        r = requests.get(url)
                        soup = BeautifulSoup(r.content, 'html5lib')
                        rows_odd = soup.find_all('tr', class_ = "odd")
                        rows_odd = rows_odd[:-3]
                        arr_td = [td.text for td in rows_odd]
                        total_elapsed_time = arr_td[4]
                        total_elapsed_time = total_elapsed_time.replace("\n","")
                        total_elapsed_time = total_elapsed_time.replace(" ","")
                        total_elapsed_time = total_elapsed_time[8:]
                        body_response['hadoop_total_elapsed_time'] = total_elapsed_time
                        average_map_time = arr_td[5]
                        average_map_time = average_map_time.replace("\n","")
                        average_map_time = average_map_time.replace(" ","")
                        average_map_time = average_map_time[14:]
                        body_response['hadoop_average_map_time'] = average_map_time
                        average_merge_time = arr_td[6]
                        average_merge_time = average_merge_time.replace("\n","")
                        average_merge_time = average_merge_time.replace(" ","")
                        average_merge_time = average_merge_time[-4:]
                        body_response['hadoop_average_merge_time'] = average_merge_time

                        rows_even = soup.find_all('tr', class_ = "even")
                        rows_even = rows_even[4:]
                        arr_td_even = [td.text for td in rows_even]
                        average_shuffle_time = arr_td_even[1]
                        average_shuffle_time = average_shuffle_time.replace("\n","")
                        average_shuffle_time = average_shuffle_time.replace(" ","")
                        average_shuffle_time = average_shuffle_time[-4:]
                        body_response['hadoop_average_shuffle_time'] = average_shuffle_time
                        average_reduce_time = arr_td_even[2]
                        average_reduce_time = average_reduce_time.replace("\n","")
                        average_reduce_time = average_reduce_time.replace(" ","")
                        average_reduce_time = average_reduce_time[-4:]
                        body_response['hadoop_average_reduce_time'] = average_reduce_time

                         #spark code
                        table_name_spark = table_name.lower()
                        columns_spark = selected_columns
                        function_spark = "min"
                        conditon_spark = fun_column
                        having_conditon_spark = having_condition
                        join_table_spark_1 = table_name_spark +".py"
                        args_list_spark = ['python3', join_table_spark_1, columns_spark, function_spark, conditon_spark, having_conditon_spark]
                        result2 = run_cmd(args_list_spark)
                        #print("i am here")
                        if(int(result2)==0):
                            if(os.path.exists("part-00000")):
                                os.remove("part-00000")
                            #combination_tables = combination_tables.lower()
                            path_hdfs = '/cloud/output/spark/' + table_name_spark + '/part-00000'
                            path_local = '/home/HDUSER/Desktop/flask_app/cloud/'
                            args_list_4 = ['hadoop','fs','-copyToLocal', path_hdfs, path_local]
                            result3 = run_cmd(args_list_4)
                        if(int(result3) == 0):
                            arr = []
                            with open('part-00000') as output_file:
                                for lines in output_file:
                                    lines = lines.replace("\t",",")
                                    lines = lines.replace('"','')
                                    lines = lines.replace("(","")
                                    lines = lines.replace(")","")
                                    lines = lines.replace("'","")
                                    arr.append(lines.strip())
                                body_response['spark_query_result'] = arr
                        execution_time_spark = ""
                        with open('extime.txt') as f:
                            for lines in f:
                                execution_time_spark = lines
                        body_response["spark_time_execution"] = execution_time_spark
                        respData = body_response
            if("sum" in lower_inStr):
                if("users" or "rating" in lower_inStr):
                    result = ""
                    result1 = ""
                    strArr = lower_inStr.split(" ")
                    users_table_columns = ["userid", "age", "gender", "occupation", "zipcode"]
                    rating_table_columns = ["userid", "movieid", "rating", "timestamp"]
                    selected_columns = strArr[1]
                    selected_columns = selected_columns[:-1]
                    selected_columns_arr = selected_columns.split(",")
                    table_name = strArr[4]
                    table_name = table_name.title()
                    fun_column = strArr[2]
                    fun_column = fun_column[4:-1]
                    group_by_columns = strArr[7]
                    group_by_columns_arr = group_by_columns.split(",")
                    having_condition_1 = strArr[9]
                    having_condition = strArr[11]
                    having_cond_fun_col = having_condition_1[4:-1]
                    #print(having_cond_fun_col, len(having_cond_fun_col))
                    #print(having_condition_1)
                    for s in group_by_columns_arr:
                        if table_name =="Users" and s not in users_table_columns:
                            print("failing due to users table name")
                            invalidQuery = True
                            return jsonify({'invalidQuery':"failing due to users table name in group by clause"})
                    for s in group_by_columns_arr:
                        if table_name =="Rating" and s not in users_table_columns:
                            print("failing due to Rating table name")
                            invalidQuery = True
                            return jsonify({'invalidQuery':"failing due to Rating table name in group by clause"})
                    if(selected_columns != group_by_columns):
                        print("selected_columns",selected_columns)
                        print("group_by_columns", group_by_columns)
                        invalidQuery = True
                        return jsonify({'invalidQuery':"failing due to mismatch between select columns and groupby columns"})
                    if(fun_column == "age" or fun_column == "rating"):
                        invalidQuery = False
                        #return jsonify({'invalidQuery':"This is an Invalid Query"})
                    else:
                        print(fun_column)
                        print("failing due to inappropriate column in function")
                        invalidQuery = True
                        return jsonify({'invalidQuery':"failing due to inappropriate column in function"})
                    if(having_cond_fun_col != fun_column):
                        invalidQuery = True
                        return jsonify({'invalidQuery':"failing due to mismatch in column having condition function and selection function"})
                    if(invalidQuery == False):
                        join_table_2 = join_table_2+"SumGroupByMain"+table_name
                        args_list = ['hadoop', 'jar', 'groupBy.jar', join_table_2, selected_columns, fun_column, group_by_columns, having_condition]
                        result = run_cmd(args_list)
                    if(int(result)==0):
                        if(os.path.exists("part-r-00000")):
                            os.remove("part-r-00000")
                        table_name = table_name.lower()
                        path_hdfs = '/cloud/output/' + table_name + '/part-r-00000'
                        path_local = '/home/HDUSER/Desktop/flask_app/cloud/'
                        args_list_2 = ['hadoop','fs','-copyToLocal', path_hdfs, path_local]
                        result1 = run_cmd(args_list_2)
                        #print(result1)
                    if(int(result1) == 0):
                        arr = []
                        with open('part-r-00000') as output_file:
                            for lines in output_file:
                                lines = lines.replace("\t",",")
                                lines = lines.replace('"','')
                                lines = lines.strip()
                                #lines = lines[3:]
                                arr.append(lines)
                            final_arr = []
                            for x in arr:
                                x_arr = x.split(",")
                                str2 =""
                                length = len(group_by_columns_arr)
                                for i in range(length, len(x_arr), 1):
                                    str2 = str2 + x_arr[i] + ","
                                str2 = str2[:-1]
                                final_arr.append(str2)
                                #print(x_arr)
                            body_response['hadoop_query_result'] = final_arr
                        path = "/usr/local/hadoop/logs/userlogs"
                        all_subdirs = [d for d in os.listdir(path)]
                        all_subdirs.sort(reverse = True)
                        process = all_subdirs[0]
                        #print(all_subdirs)
                        body_response['process_name'] = process
                        job = "job_"+process[12:]
                        url = "http://localhost:19888/jobhistory/job/"+job
                        r = requests.get(url)
                        soup = BeautifulSoup(r.content, 'html5lib')
                        rows_odd = soup.find_all('tr', class_ = "odd")
                        rows_odd = rows_odd[:-3]
                        arr_td = [td.text for td in rows_odd]
                        total_elapsed_time = arr_td[4]
                        total_elapsed_time = total_elapsed_time.replace("\n","")
                        total_elapsed_time = total_elapsed_time.replace(" ","")
                        total_elapsed_time = total_elapsed_time[8:]
                        body_response['hadoop_total_elapsed_time'] = total_elapsed_time
                        average_map_time = arr_td[5]
                        average_map_time = average_map_time.replace("\n","")
                        average_map_time = average_map_time.replace(" ","")
                        average_map_time = average_map_time[14:]
                        body_response['hadoop_average_map_time'] = average_map_time
                        average_merge_time = arr_td[6]
                        average_merge_time = average_merge_time.replace("\n","")
                        average_merge_time = average_merge_time.replace(" ","")
                        average_merge_time = average_merge_time[-4:]
                        body_response['hadoop_average_merge_time'] = average_merge_time

                        rows_even = soup.find_all('tr', class_ = "even")
                        rows_even = rows_even[4:]
                        arr_td_even = [td.text for td in rows_even]
                        average_shuffle_time = arr_td_even[1]
                        average_shuffle_time = average_shuffle_time.replace("\n","")
                        average_shuffle_time = average_shuffle_time.replace(" ","")
                        average_shuffle_time = average_shuffle_time[-4:]
                        body_response['hadoop_average_shuffle_time'] = average_shuffle_time
                        average_reduce_time = arr_td_even[2]
                        average_reduce_time = average_reduce_time.replace("\n","")
                        average_reduce_time = average_reduce_time.replace(" ","")
                        average_reduce_time = average_reduce_time[-4:]
                        body_response['hadoop_average_reduce_time'] = average_reduce_time

                        #spark code
                        table_name_spark = table_name.lower()
                        columns_spark = selected_columns
                        function_spark = "sum"
                        conditon_spark = fun_column
                        having_conditon_spark = having_condition
                        join_table_spark_1 = table_name_spark +".py"
                        args_list_spark = ['python3', join_table_spark_1, columns_spark, function_spark, conditon_spark, having_conditon_spark]
                        result2 = run_cmd(args_list_spark)
                        #print("i am here")
                        if(int(result2)==0):
                            if(os.path.exists("part-00000")):
                                os.remove("part-00000")
                            #combination_tables = combination_tables.lower()
                            path_hdfs = '/cloud/output/spark/' + table_name_spark + '/part-00000'
                            path_local = '/home/HDUSER/Desktop/flask_app/cloud/'
                            args_list_4 = ['hadoop','fs','-copyToLocal', path_hdfs, path_local]
                            result3 = run_cmd(args_list_4)
                        if(int(result3) == 0):
                            arr = []
                            with open('part-00000') as output_file:
                                for lines in output_file:
                                    lines = lines.replace("\t",",")
                                    lines = lines.replace('"','')
                                    lines = lines.replace("(","")
                                    lines = lines.replace(")","")
                                    lines = lines.replace("'","")
                                    arr.append(lines.strip())
                                final_arr = []
                                for x in arr:
                                    pass
                                    #print(x)
                                    #x_arr = x.split(",")
                                    #str2 =""
                                    #length = len(group_by_columns_arr)
                                    #for i in range(length, len(x_arr), 1):
                                    #    str2 = str2 + x_arr[i] + ","
                                    #str2 = str2[:-1]
                                    #final_arr.append(str2)
                                    #print(x_arr)
                                body_response['spark_query_result'] = arr
                        execution_time_spark = ""
                        with open('extime.txt') as f:
                            for lines in f:
                                execution_time_spark = lines
                        body_response["spark_time_execution"] = execution_time_spark
                        respData = body_response
            if("count" in lower_inStr):
                if("users" or "rating" or "movies" or "zipcodes" in lower_inStr):
                    result = ""
                    result1 = ""
                    strArr = lower_inStr.split(" ")
                    users_table_columns = ["userid", "age", "gender", "occupation", "zipcode"]
                    rating_table_columns = ["userid", "movieid", "rating", "timestamp"]
                    selected_columns = strArr[1]
                    selected_columns = selected_columns[:-1]
                    selected_columns_arr = selected_columns.split(",")
                    table_name = strArr[4]
                    table_name = table_name.title()
                    fun_column = strArr[2]
                    fun_column = fun_column[4:-1]
                    group_by_columns = strArr[7]
                    group_by_columns_arr = group_by_columns.split(",")
                    having_condition_1 = strArr[9]
                    having_condition = strArr[11]
                    having_cond_fun_col = having_condition_1[4:-1]
                    #print(having_cond_fun_col, len(having_cond_fun_col))
                    #print(having_condition_1)
                    if(selected_columns != group_by_columns):
                        print("selected_columns",selected_columns)
                        print("group_by_columns", group_by_columns)
                        invalidQuery = True
                        return jsonify({'invalidQuery':"failing due to mismatch between select columns and groupby columns"})
                    
                    if(having_cond_fun_col != fun_column):
                        invalidQuery = True
                        return jsonify({'invalidQuery':"failing due to mismatch in column having condition function and selection function"})
                    if(invalidQuery == False):
                        join_table_2 = join_table_2+"CountGroupByMain"+table_name
                        args_list = ['hadoop', 'jar', 'groupBy.jar', join_table_2, selected_columns, group_by_columns, having_condition]
                        result = run_cmd(args_list)
                    if(int(result)==0):
                        if(os.path.exists("part-r-00000")):
                            os.remove("part-r-00000")
                        table_name = table_name.lower()
                        path_hdfs = '/cloud/output/' + table_name + '/part-r-00000'
                        path_local = '/home/HDUSER/Desktop/flask_app/cloud/'
                        args_list_2 = ['hadoop','fs','-copyToLocal', path_hdfs, path_local]
                        result1 = run_cmd(args_list_2)
                        #print(result1)
                    if(int(result1) == 0):
                        arr = []
                        with open('part-r-00000') as output_file:
                            for lines in output_file:
                                lines = lines.replace("\t",",")
                                lines = lines.replace('"','')
                                lines = lines.strip()
                                #lines = lines[3:]
                                arr.append(lines)
                            body_response['hadoop_query_result'] = arr
                        path = "/usr/local/hadoop/logs/userlogs"
                        all_subdirs = [d for d in os.listdir(path)]
                        all_subdirs.sort(reverse = True)
                        process = all_subdirs[0]
                        #print(all_subdirs)
                        body_response['process_name'] = process
                        job = "job_"+process[12:]
                        url = "http://localhost:19888/jobhistory/job/"+job
                        r = requests.get(url)
                        soup = BeautifulSoup(r.content, 'html5lib')
                        rows_odd = soup.find_all('tr', class_ = "odd")
                        rows_odd = rows_odd[:-3]
                        arr_td = [td.text for td in rows_odd]
                        total_elapsed_time = arr_td[4]
                        total_elapsed_time = total_elapsed_time.replace("\n","")
                        total_elapsed_time = total_elapsed_time.replace(" ","")
                        total_elapsed_time = total_elapsed_time[8:]
                        body_response['hadoop_total_elapsed_time'] = total_elapsed_time
                        average_map_time = arr_td[5]
                        average_map_time = average_map_time.replace("\n","")
                        average_map_time = average_map_time.replace(" ","")
                        average_map_time = average_map_time[14:]
                        body_response['hadoop_average_map_time'] = average_map_time
                        average_merge_time = arr_td[6]
                        average_merge_time = average_merge_time.replace("\n","")
                        average_merge_time = average_merge_time.replace(" ","")
                        average_merge_time = average_merge_time[-4:]
                        body_response['hadoop_average_merge_time'] = average_merge_time

                        rows_even = soup.find_all('tr', class_ = "even")
                        rows_even = rows_even[4:]
                        arr_td_even = [td.text for td in rows_even]
                        average_shuffle_time = arr_td_even[1]
                        average_shuffle_time = average_shuffle_time.replace("\n","")
                        average_shuffle_time = average_shuffle_time.replace(" ","")
                        average_shuffle_time = average_shuffle_time[-4:]
                        body_response['hadoop_average_shuffle_time'] = average_shuffle_time
                        average_reduce_time = arr_td_even[2]
                        average_reduce_time = average_reduce_time.replace("\n","")
                        average_reduce_time = average_reduce_time.replace(" ","")
                        average_reduce_time = average_reduce_time[-4:]
                        body_response['hadoop_average_reduce_time'] = average_reduce_time

                        #spark code
                        table_name_spark = table_name.lower()
                        columns_spark = selected_columns
                        function_spark = "count"
                        conditon_spark = "random"
                        having_conditon_spark = having_condition
                        join_table_spark_1 = table_name_spark +".py"
                        args_list_spark = ['python3', join_table_spark_1, columns_spark, function_spark, conditon_spark, having_conditon_spark]
                        result2 = run_cmd(args_list_spark)
                        #print("i am here")
                        if(int(result2)==0):
                            if(os.path.exists("part-00000")):
                                os.remove("part-00000")
                            #combination_tables = combination_tables.lower()
                            path_hdfs = '/cloud/output/spark/' + table_name_spark + '/part-00000'
                            path_local = '/home/HDUSER/Desktop/flask_app/cloud/'
                            args_list_4 = ['hadoop','fs','-copyToLocal', path_hdfs, path_local]
                            result3 = run_cmd(args_list_4)
                        if(int(result3) == 0):
                            arr = []
                            with open('part-00000') as output_file:
                                for lines in output_file:
                                    lines = lines.replace("\t",",")
                                    lines = lines.replace('"','')
                                    arr.append(lines.strip())
                                final_arr = []
                                for x in arr:
                                    x = x.replace("(","")
                                    x = x.replace("'","")
                                    x = x.replace(")","")
                                    final_arr.append(x)
                                    #print(x_arr)
                                body_response['spark_query_result'] = final_arr
                        execution_time_spark = ""
                        with open('extime.txt') as f:
                            for lines in f:
                                execution_time_spark = lines
                        body_response["spark_time_execution"] = execution_time_spark
                        respData = body_response
        return jsonify(respData), 200
    return app