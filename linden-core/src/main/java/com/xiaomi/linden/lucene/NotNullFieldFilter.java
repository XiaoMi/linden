package com.xiaomi.linden.lucene;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import com.xiaomi.linden.lucene.search.LindenFieldCacheImpl;

/**
 * Created by yozhao on 18/08/2017.
 */
public class NotNullFieldFilter extends Filter {

  private final String fieldName;

  /**
   * @param fieldName The filed documents need to have no value in order to be a match for this filter.
   */
  public NotNullFieldFilter(String fieldName) {
    this.fieldName = fieldName;
  }


  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    DocIdSet docIdSet = LindenFieldCacheImpl.DEFAULT.getNotNullFieldDocIdSet(context.reader(), fieldName);
    return BitsFilteredDocIdSet.wrap(docIdSet, acceptDocs);
  }
}
