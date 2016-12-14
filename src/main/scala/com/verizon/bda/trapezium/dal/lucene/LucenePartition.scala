package com.verizon.bda.trapezium.dal.lucene

import org.apache.lucene.index.LeafReader
import org.apache.spark.Partition

/**
 * @author debasish83 on 12/13/16.
 */
class LucenePartition(index: Int, reader: LeafReader) extends Partition {

}
