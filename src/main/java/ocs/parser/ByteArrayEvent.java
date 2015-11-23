package ocs.parser;

import reactor.event.Event;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sgn on 23/11/15.
 */
public class ByteArrayEvent extends Event<byte[]> {

    private static final String HEADER_FILENAME = "filename";

    private static Event.Headers createHeader(String filename){
        Map<String, String> header = new HashMap<String, String>();

        header.put(HEADER_FILENAME, filename);

        return new Event.Headers(header);
    }

    public ByteArrayEvent(File file, byte[] data) {
        super(createHeader(file.getAbsolutePath()), data);
    }

    public String getFilenameHeader(){
        return this.getHeaders().get(HEADER_FILENAME);
    }
}
