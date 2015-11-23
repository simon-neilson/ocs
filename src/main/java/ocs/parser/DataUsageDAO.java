package ocs.parser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sgn on 23/11/15.
 */
@Service
public class DataUsageDAO {

    private static final Log log = LogFactory.getLog(LineConsumer.class);

    private static final Map<String, String> map = Collections.synchronizedMap(new HashMap<String, String>());

    public Map<String, String> getMap(){
        return map;
    }
}
