package com.lsk.bigdata.spark.core.framework.common

import com.lsk.bigdata.spark.core.framework.util.EnvUtil


trait TDao {

    def readFile(path:String) = {
        EnvUtil.take().textFile(path)
    }
}
