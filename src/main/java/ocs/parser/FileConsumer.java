package ocs.parser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.spring.annotation.Consumer;
import reactor.spring.annotation.ReplyTo;
import reactor.spring.annotation.Selector;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.zip.GZIPInputStream;

/**
 * Created by sgn on 23/11/15.
 */
//@Consumer
public class FileConsumer {

    private static final Log log = LogFactory.getLog(FileConsumer.class);

//    private static final NumberFormat nf = new DecimalFormat("0.000");
//
//    @Autowired
//    @Qualifier("fileProcessorReactor")
//    public Reactor reactor;

//    @Autowired
//    private Reactor lineProcessorReactor;

    //@Selector(Parser.TOPIC_INPUT_FILES)
    //@ReplyTo("reply.topic")
//    public String processFile(ByteArrayEvent evt) {
//
//        String arrayLength = nf.format(((double) evt.getData().length / 1000000));
//
//        String filename = evt.getFilenameHeader();
//
//        log.info("Processing contents of filename: " + filename + ", byte[] length: " + arrayLength + " ...");
//
//        try {
//            InputStream byteStream = new ByteArrayInputStream(evt.getData());
//            InputStream gzipStream = new GZIPInputStream(byteStream);
//            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
//            BufferedReader buffered = new BufferedReader(decoder);
//
//            int lineNumber = 0;
//            String line;
//            while ((line = buffered.readLine()) != null) {
//                // process the line.
//                //TODO apply the regex, convert to 8 field CSV
//                String content = filename+"\n" + ++lineNumber + "\n" + line;
//                //log.info("\n" + content);
////                for(Registration r : lineProcessorReactor.getConsumerRegistry()){
////                    log.info("lineProcessor: " + r.getObject()) ;
////                }
//                reactor.send(Parser.TOPIC_INPUT_LINES, Event.wrap(content));
//            }
//
//            log.info("Processing contents of filename: " + filename + ", byte[] length: " + arrayLength + " DONE");
//        }catch(Exception e){
//            throw new RuntimeException("Error streaming filename: " + filename, e);
//        }
//
//        return filename;
//    }
}