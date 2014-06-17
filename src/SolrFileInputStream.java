import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
  List<String> parts; 
  
  void fetchMeta() throws SolrServerException {
    ModifiableSolrParams query = new ModifiableSolrParams();
    query.set("q", "name:"+filename);
    query.set("rows", "1");
    
    QueryResponse resp = server.query(query);
    SolrDocumentList res = resp.getResults();

    if(res.size()==0) {
      return;
    } else {
      for (SolrDocument d : res) {
        parts = new ArrayList<>();
        for(Object v: d.getFieldValues("parts")) {
          parts.add(v.toString());
        }
        break;
      }
    }
  }
  
  final int BATCH_SIZE = 10;
  
  private ByteBuffer fetch() throws SolrServerException {
    if(parts==null || parts.size()==0 || start>=parts.size())
      return null;
    
    ModifiableSolrParams query = new ModifiableSolrParams();
    StringBuffer queryStr = new StringBuffer("id:"+parts.get(start));
    for(int i=start+1; i<start+BATCH_SIZE && i<parts.size(); i++)
      queryStr.append(" OR id:"+parts.get(i));
    
    query.set("q", queryStr.toString());
    query.set("sort", "sequence asc");

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
    if(parts==null) {
      try {
        fetchMeta();
      } catch (SolrServerException e) {
        e.printStackTrace();
      }
      if(parts==null)
        return -1;
    }
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
