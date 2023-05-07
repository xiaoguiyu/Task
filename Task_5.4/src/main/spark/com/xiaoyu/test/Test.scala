package com.xiaoyu.test

import com.ibm.icu.text.SimpleDateFormat
import com.ibm.icu.util.Calendar


object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")


    val calendar: Calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    println(new SimpleDateFormat("yyyyMMdd").format(calendar))



  }
}
