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
  * Trait for key-value data store. This trait defines additional functions
  * to read/write partial table/collection based on the key.
  *
  * @tparam K key type.
  * @tparam V value type returned during point look-ups.
  */
trait KVData[K, V] {

  /**
    * Fetch the value for the given key and return it.
    *
    * @param key
    * @return V
    */
  def get(key: K): Option[V]


  /**
    * Insert a row containing key and value pair into the data store.
    *
    * @param key
    * @param value
    */
  def put(key: K, value: V): Unit

  /**
    * Delete a value using key
    * @param key
    */
  def delete(key : K)

}

