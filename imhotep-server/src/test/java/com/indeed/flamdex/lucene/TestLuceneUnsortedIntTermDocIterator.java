package com.indeed.flamdex.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
public class TestLuceneUnsortedIntTermDocIterator {
    @Test
    public void testSingleTerm() throws IOException {
        RAMDirectory d = new RAMDirectory();
        IndexWriter w = new IndexWriter(d, null, true, IndexWriter.MaxFieldLength.LIMITED);
        Document doc = new Document();
        doc.add(new Field("int", "1", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
        w.addDocument(doc);
        w.close();

        IndexReader r = IndexReader.open(d);
        LuceneUnsortedIntTermDocIterator iter = LuceneUnsortedIntTermDocIterator.create(r, "int");
        assertTrue(iter.nextTerm());
        assertEquals(1, iter.term());
        int[] docs = new int[2];
        assertEquals(1, iter.nextDocs(docs));
        assertEquals(0, docs[0]);
        assertFalse(iter.nextTerm());
        r.close();
    }
}
