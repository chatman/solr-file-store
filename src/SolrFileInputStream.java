import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;


public class SolrFileInputStream extends InputStream {

  final SolrServer server;
  final String filename;
  
  public SolrFileInputStream(SolrServer server, String filename) {
    this.server = server;
    this.filename = filename;
  }

  ByteBuffer buffer;
  int position = 0;
  
  int start = 0;
  long size = 0;
  
  private ByteBuffer fetch() throws SolrServerException {
    ModifiableSolrParams query = new ModifiableSolrParams();
    query.set("q", "name:"+filename);
    query.set("rows", "10");
    query.set("sort", "part asc");
    query.set("start", start);

    QueryResponse resp = server.query(query);
    SolrDocumentList res = resp.getResults();

    if(res.size()==0) {
      return null;
    }
    
    int bufferSize = 0;
    for (SolrDocument d : res) {
      for(Object v: d.getFieldValues("rawdata")) {
        byte[] data = (byte[]) v;
        size+=data.length;
        bufferSize+=data.length;
      }
    }

    if(buffer==null)
      buffer = ByteBuffer.allocate(bufferSize);
    buffer.flip();
    buffer.clear();
    buffer.limit(bufferSize);
        
    for (SolrDocument d : res) {
      for(Object v: d.getFieldValues("rawdata")) {
        byte[] data = (byte[]) v;
        for(int i=0; i<data.length; i++) {
          buffer.put(data[i]);
        }
      }
    }

    start+=res.size();
        return buffer;
  }

  @Override
  public int read() throws IOException {
    if(buffer==null || buffer.limit()==position) {
      try {
        if(fetch()==null)
          return -1;
      } catch (SolrServerException e) {
        e.printStackTrace();
        return -1;
      }
      
      if(buffer==null)
        return -1;
      
      position = 0; 
    }

    return (256+buffer.get(position++))%256;
  }
  
  @Override
  public void close() throws IOException {
    buffer = null;
  }

}
