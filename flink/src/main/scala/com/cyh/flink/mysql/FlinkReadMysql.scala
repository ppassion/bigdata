package com.cyh.flink.mysql

import java.sql.{Connection, DriverManager}


object FlinkReadMysql {

    var connection: Connection = null

    def connect():Unit = {
        val username = "root"
        val password = "123456"
        val drive = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://192.168.52.102:3306/bigdata"
        try {
            connection = DriverManager.getConnection(url, username, password)
        } catch {
            case e: Exception=> e.printStackTrace()
        } finally {

        }
    }

    def select():Unit = {
        try {
            val statement = connection.createStatement()
            val resultSet = statement.executeQuery("select * from user")
            println("id\tname")
            while (resultSet.next()) {
                println(resultSet.getString(1) + "\t" + resultSet.getString(2))
            }
            println("-------------")
        } catch {
            case e: Exception=> e.printStackTrace()
        } finally {

        }
    }

    def close():Unit = {
        connection.close()
    }

    def main(args: Array[String]): Unit = {
        connect()
        while(true) {
            select()
            Thread.sleep(10000)
        }
        close()
    }
}
