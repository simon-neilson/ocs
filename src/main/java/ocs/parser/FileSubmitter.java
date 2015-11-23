package ocs.parser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.Reactor;
import reactor.event.Event;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Created by sgn on 23/11/15.
 */
@Service
public class FileSubmitter {

    private static final Log log = LogFactory.getLog(FileSubmitter.class);

    private static final NumberFormat nf = new DecimalFormat("0.000");

    private static final Pattern pattern = Pattern.compile("^.*?\\,143,4605,3\\>.*?\\,330,4605,3\\>\\[\\d*\\,\\d*\\,\\d*\\,(.*?)\\,.*?\\,330,4605,3\\>\\[\\d*\\,\\d*\\,\\d*\\,(.*?)\\,.*?\\,330,4605,3\\>\\[\\d*\\,\\d*\\,\\d*\\,(.*?)\\,.*?\\,625,4605,3\\>\\[.*?\\,.*?\\,(.*?)\\,.*\\}\\,\\d\\d\\d\\d\\-\\d\\d\\-\\d\\d.*?\\,(.*?)\\,.*?\\,-10,4605,3\\>\\[(\\d+).*?\\,646\\,4605\\,3\\>.*?\\:(.*?)\\)\\,(.*?)[\\,|\\]]");

    @Autowired
    private Reactor fileProcessorReactor;

    public void submitFile(File file){

        String fileLength = nf.format(((double) file.length() / 1000000));

        log.info("Loading file: " + file.getAbsolutePath() + " into byte[], size (MB)=" + fileLength + " and submitting to file processing reactor ...");

        try {
//            fileProcessorReactor.notify(
//                    Parser.TOPIC_INPUT_FILES,
//                    new ByteArrayEvent(
//                            file,
//                            Files.readAllBytes(Paths.get(file.toURI()))));
            processFile(new ByteArrayEvent(
                            file,
                            Files.readAllBytes(Paths.get(file.toURI()))));
        }catch(Exception e){
            throw new RuntimeException("Error loading file: " + file.getAbsolutePath() + " into byte[]", e);
        }

        log.info("Loading file: " + file.getAbsolutePath() + " into byte[], size (MB)=" + fileLength + " and submitting to file processing reactor DONE");

    }

    public String processFile(ByteArrayEvent evt) {

        String arrayLength = nf.format(((double) evt.getData().length / 1000000));

        String filename = evt.getFilenameHeader();

        log.info("Processing contents of filename: " + filename + ", byte[] length: " + arrayLength + " ...");

        try {
            InputStream byteStream = new ByteArrayInputStream(evt.getData());
            InputStream gzipStream = new GZIPInputStream(byteStream);
            Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
            BufferedReader buffered = new BufferedReader(decoder);

            int lineNumber = 0;
            String line;
            while ((line = buffered.readLine()) != null) {
                // process the line.

                String decodedLine = decodeLine(line);

                if(decodedLine==null) continue;

                String content = filename+"\n" + ++lineNumber + "\n" + decodedLine;
                //log.info("\n" + content);

                fileProcessorReactor.notify(Parser.TOPIC_INPUT_LINES, Event.wrap(content));
            }

            log.info("Processing contents of filename: " + filename + ", byte[] length: " + arrayLength + " DONE");
        }catch(Exception e){
            throw new RuntimeException("Error streaming filename: " + filename, e);
        }

        return filename;
    }

    public String decodeLine(String line){

        Matcher matcher = pattern.matcher(line);

        if(!matcher.find()){
            //log.info("regex miss for: " + line);
            return null;
        }

        String formatted = MessageFormat.format("{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}", matcher.group(1), matcher.group(2),matcher.group(3),matcher.group(4),matcher.group(5),matcher.group(6),matcher.group(7),matcher.group(8));

        return formatted;
    }
}
