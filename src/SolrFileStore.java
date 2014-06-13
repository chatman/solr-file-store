import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;


public class SolrFileStore {

  final static int BUFFER_SIZE = 1024*1024*15;

  static int streamToSolr(String name, String path, SolrServer server) throws IOException, SolrServerException {
    RandomAccessFile aFile = new RandomAccessFile(path, "r");
    FileChannel inChannel = aFile.getChannel();
    ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

    int counter = 0;
    
    server.deleteByQuery("name:"+name);
    server.commit();
    
    while(inChannel.read(buffer) > 0)
    {
      buffer.flip();
      counter++;
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", name+"_"+counter);
      doc.addField("name", name);
      doc.addField("rawdata", ByteBuffer.wrap(buffer.array().clone(), 0, buffer.remaining()));
      doc.addField("part", counter);
      System.out.println(name+"_"+counter);
      if(counter%2==0) {
        server.commit();
        System.out.println("Committing..");
      }
      server.add(doc);
      buffer.clear();
    }
    
    inChannel.close();
    aFile.close();
    server.commit();
    System.out.println("Committed...");

    return counter;
  }

  public static void main(String[] args) throws SolrServerException, IOException {
    SolrServer server = new HttpSolrServer("http://localhost:8983/solr");

    long startTime = System.currentTimeMillis();
    streamToSolr("en-pos-maxent.bin", "en-pos-maxent.bin", server);
    long endTime = System.currentTimeMillis();

    System.out.println("Indexing time: "+(endTime-startTime));

    startTime = System.currentTimeMillis();
    FileOutputStream fos = new FileOutputStream(new File("test.out"));
    InputStream is = new SolrFileInputStream(server, "en-pos-maxent.bin");

    IOUtils.copy(is, fos);
    
    //is.close();
    fos.close();
    endTime = System.currentTimeMillis();
    System.out.println("Retrieval time: "+(endTime-startTime));
  }
}
