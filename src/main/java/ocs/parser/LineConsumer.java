package ocs.parser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import reactor.core.Reactor;
import reactor.event.Event;
import reactor.spring.annotation.Consumer;
import reactor.spring.annotation.Selector;

import java.io.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Created by sgn on 23/11/15.
 */
@Consumer
public class LineConsumer {

    private static final Log log = LogFactory.getLog(LineConsumer.class);

    private static AtomicLong counter = new AtomicLong();

    @Autowired
    DataUsageDAO dataUsageDAO;

    @Autowired
    @Qualifier("fileProcessorReactor")
    public Reactor reactor;

    @Selector(Parser.TOPIC_INPUT_LINES)
    //@ReplyTo("reply.topic")
    public String processLine(Event evt) {

        String line = (String) evt.getData();

        log.info("Consuming contents of event: " + line);
    // 709480.0, 191343.0, 518137.0, 0:5:5:31971973, 2015-11-19T21:00:02.000000+08:00, 41, @AUD_IT, 0:4:19:334483522
        //log.info(counter.incrementAndGet());

        String[] fields = line.split(",");

        String recordId = fields[7];

        String value = dataUsageDAO.getMap().get(recordId);

        if(value == null){
//            dataUsageDAO.getMap().put(recordId, line);
        }else{
//            dataUsageDAO.getMap().put(recordId, value + "\n" + line);
        }

        return "";
    }
}