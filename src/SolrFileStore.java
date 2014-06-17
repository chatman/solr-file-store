import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;


public class SolrFileStore {

  final static int BUFFER_SIZE = 1024*1024*15;

  static int streamToSolr(String name, String path, SolrServer server) throws IOException, SolrServerException, NoSuchAlgorithmException {
    RandomAccessFile aFile = new RandomAccessFile(path, "r");
    FileChannel inChannel = aFile.getChannel();
    ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

    int counter = 0;
    long size = 0;

    String uuid = UUID.randomUUID().toString();
    
    List<String> ids = new ArrayList<>();
    
    while(inChannel.read(buffer) > 0)
    {
      buffer.flip();
      counter++;
      SolrInputDocument partDoc = new SolrInputDocument();
      
      partDoc.addField("rawdata", ByteBuffer.wrap(buffer.array().clone(), 0, buffer.remaining()));
      partDoc.addField("sequence", counter);
      
      size+=buffer.remaining();
      
      // Generate md5 for part
      /*StringBuffer md5 = new StringBuffer();
      try {
        java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
        byte[] array = md.digest((new String(buffer.array())).getBytes("UTF-8"));
        for (int i = 0; i < array.length; ++i) {
          md5.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1,3));
        }
      } catch (java.security.NoSuchAlgorithmException e) {
        
      }*/
      
      String partId = uuid+"_"+counter;
      partDoc.addField("id", partId);
      ids.add(partId);

      if(counter%2==0) {
        server.commit();
        System.out.println("Committing.. "+"Parts written: "+counter);
      }
      server.add(partDoc);
      buffer.clear();
    }

    inChannel.close();
    aFile.close();
    server.commit();
    System.out.println("Committed "+counter+" parts...");
    
    
    server.deleteByQuery("name:"+name);
    SolrInputDocument metaDoc = new SolrInputDocument();
    metaDoc.addField("id", uuid);
    metaDoc.addField("name", name);
    for(String p: ids)
      metaDoc.addField("parts", p);
    metaDoc.addField("size", size); 
    server.add(metaDoc);
    server.commit();
    System.out.println("Committed meta document: "+metaDoc);
    
    return counter;
  }

  public static void main(String[] args) throws SolrServerException, IOException, NoSuchAlgorithmException {
    SolrServer server = new HttpSolrServer("http://localhost:8983/solr");

    long startTime = System.currentTimeMillis();
    streamToSolr("xp.iso", "xp.iso", server);
    long endTime = System.currentTimeMillis();

    System.out.println("Indexing time: "+(endTime-startTime));

    startTime = System.currentTimeMillis();
    FileOutputStream fos = new FileOutputStream(new File("test.out"));
    InputStream is = new SolrFileInputStream(server, "xp.iso");

    IOUtils.copy(is, fos);

    //is.close();
    fos.close();
    endTime = System.currentTimeMillis();
    System.out.println("Retrieval time: "+(endTime-startTime));
  }
}
