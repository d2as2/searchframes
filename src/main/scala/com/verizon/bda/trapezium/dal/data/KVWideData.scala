/**
* Copyright (C) 2016 Verizon. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.verizon.bda.trapezium.dal.data

/**
  * Created by v468328 on 10/17/16.
  */
trait KVWideData[K, T] extends KVData[K, T] {


  /**
    * Fetch a range of key-value pairs and return it as a distributed collection of data.
    *
    * @param startRow
    * @param endRow
    * @return T
    */
  protected def getRange(startRow: K, endRow: K): T

  /**
    * Fetch n values from the data store starting with startRow and return the distributed
    * collection of data.
    *
    * @param startRow
    * @param n number of records to be fetched
    * @return T
    */
  protected def multiGet(startRow: K, n: Int): T

}
