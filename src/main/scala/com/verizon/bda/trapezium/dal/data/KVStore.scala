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

import java.io.Serializable

/**
  * Base trait of all cache and kv stores. It is generic enough.
  * It provides init as call back hook because common framework dont know
  * what is needed for initialization in advnace. For couchbase there are 2 params
  * @tparam K
  * @tparam V
  */
trait KVStore[K, V] extends KVData[K, V] with Serializable {


  def put(key: K, value: V, ttl: Int = 24 * 60 * 60)

  def putMulti(keyMap: Map[K, V], ttl: Int = 24 * 60 * 60)

  def getMulti(keyList: Seq[K]): Map[K, V]

  def init(params : String*): Unit // general purpose hook for initialization

  def flush(): Unit

}
