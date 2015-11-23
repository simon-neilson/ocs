package ocs.parser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.spring.context.config.EnableReactor;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Created by sgn on 23/11/15.
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan
@EnableReactor
public class Parser implements CommandLineRunner{

    private static final Log log = LogFactory.getLog(Parser.class);

    public static final String TOPIC_INPUT_FILES = "input-files.topic";

    public static final String TOPIC_INPUT_LINES = "input-lines.topic";

    @Bean
    public Reactor fileProcessorReactor(Environment env) {
        return Reactors.reactor().env(env).get();
    }

//    @Bean
//    public Reactor lineProcessorReactor(Environment env) {
//        return Reactors.reactor().env(env).get();
//    }

    @Autowired
    FileSubmitter fileSubmitterService;

    @Autowired
    DataUsageDAO dataUsageDAO;

    public static final void main(String ... args){
        SpringApplication.run(Parser.class);
    }

    public File[] listNewFiles(String directory){
        File dir = new File(directory);
        File [] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".gz");
            }
        });

        for (File file : files) {
            log.info("Picked up file: " + file.getAbsolutePath());
        }

        return files;
    }

    /**
     * Main
     *  Scan directory, return list of files
     *  (on bootstrap read from stager)
     *
     *  Feed each file into fileproc reactor
     *
     * FileprocReactor
     *
     *  rename file to .proc
     *  scan file, feed lines to lineproc reactor
     *  rename file .done
     *
     * LineprocReactor
     *
     *  feeds lines to dist cache
     *  1 entry per customer with 24 hr breakdown
     *  Update hourly aggregate
     *
     * Stager
     *
     * maintains .done files for day archives old files
     * writing out cache to DB asynchronously
     *
     * DAO
     * Provides access API looking at in-mem + db
     *
     */
    @Override
    public void run(String... strings) throws Exception {
        File[] files = listNewFiles("/data/ocs-data/");

        for(File file : files){
            fileSubmitterService.submitFile(file);
        }

        //Thread.sleep(10000L);

        log.info("COMPLETE! Customer count: " + dataUsageDAO.getMap().size());
    }
}
