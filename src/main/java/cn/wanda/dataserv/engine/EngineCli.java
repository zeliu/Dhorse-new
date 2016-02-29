package cn.wanda.dataserv.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import cn.wanda.dataserv.config.DPumpConfig;
import cn.wanda.dataserv.config.parse.adaptor.CollectionConfigAdaptor;
import cn.wanda.dataserv.core.ConfigLoader;
import cn.wanda.dataserv.core.ConfigParser;
import cn.wanda.dataserv.core.ObjectFactory;

import com.google.gson.Gson;

/**
 * dpump entry <br>
 * <p/>
 * exit code:<br>
 * 0 : success<br>
 * 1 : task running failed<br>
 * 2 : task config file parse failed<br>
 * 3:  target file not exist
 *
 * @author songqian
 */
@Log4j
public class EngineCli {

    private static final String DEFAULT_LOGFILE = "./dhorseinfo.log";

    /**
     * @param args
     */
    public static void main(String[] args) {

        ConfigParser configParser = new ConfigParser();
        DPumpConfig config = null;
        //args = new String[2];
        //args[0] = "-f";
        //args[1] = EngineCli.class.getResource("/jdbcoutput2.yaml").getFile();
        try {
            // parse command line
            CommandLine ci = parseCommandLine(args);
            // parse dpump config
            config = parseConf(args, ci, configParser);
        } catch (Exception e1) {
            log.info("DPump parse config file failed.");
            log.error(e1.getMessage());
            log.info(e1.getMessage(), e1);
            printUsage();
            System.exit(2);
        }

        try {
            //resolve
            configParser.resolve(config);
            //split
            List source = configParser.split(config.getSource());
            config.setSource(source);
        } catch (Exception e) {
            log.error("DPump occured error in step: resolve/split.");
            log.error(e.getMessage(), e);
            log.error("DPump Work failed!");
            System.exit(1);
        }

        try {
            Engine e = new Engine();
            e.run(config);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.error("DPump Work failed!");
            System.exit(1);
        }
    }

    private static DPumpConfig parseConf(String[] args, CommandLine ci, ConfigParser configParser)
            throws IllegalArgumentException, ParseException {

        // set log file by params
        setLogFile(ci.getOptionValue("l"));
        // parse dpump config
        String fileName = ci.getOptionValue("f");
        String configString = ci.getOptionValue("e");
        Map content = null;
        if (StringUtils.isNotBlank(fileName)) {
            log.info("load config file from " + fileName);

            // load config file, parse to map
            ConfigLoader configLoader = ObjectFactory.getInstance().createConfigLoader(ci.getOptionValue("f"));
            content = configLoader.load(ci.getOptionValue("f"));
        } else if (StringUtils.isNotBlank(configString)) {
            log.info("load config from -e option.");
            Gson gson = new Gson();
            content = gson.fromJson(configString, Map.class);
        } else {
            throw new IllegalArgumentException("argument -f -e is empty!");
        }
        handleDataTime(ci, content);

        log.info("parse config file... ");
        DPumpConfig config = configParser.parse(new CollectionConfigAdaptor(content));
        return config;
    }

    private static void handleDataTime(CommandLine ci, Map content) {
        String datatime = ci.getOptionValue("t");
        if (StringUtils.isNotBlank(datatime)) {
            Map contextMap = (Map) content.get("context");
            if (contextMap == null) {
                contextMap = new HashMap();
                content.put("context", contextMap);
            }
            Map runtime = (Map) contextMap.get("runtime");
            if (runtime == null) {
                runtime = new HashMap();
                contextMap.put("runtime", runtime);
            }
            runtime.put("datatime", datatime);
        }
    }

    private static CommandLine parseCommandLine(String[] args)
            throws IllegalArgumentException, ParseException {
        Options ops = new Options();
        Option fileOpt = OptionBuilder.withArgName("filename").hasArg()
                .withDescription("the name of control file").create("f");
        Option execOpt = OptionBuilder.withArgName("config").hasArg()
                .withDescription("the string of config").create("e");
        Option logfileOpt = OptionBuilder.withArgName("logfilename").hasArg()
                .withDescription("the name of log file").create("l");
        Option datatimeOpt = OptionBuilder.withArgName("datatime").hasArg()
                .withDescription("the name of datatime").create("t");
        ops.addOption(fileOpt);
        ops.addOption(execOpt);
        ops.addOption(logfileOpt);
        ops.addOption(datatimeOpt);
        CommandLineParser parser = new PosixParser();
        CommandLine ci = parser.parse(ops, args);
        return ci;
    }

    /**
     * set log target by params
     *
     * @param logFileName
     */
    private static void setLogFile(String logFileName) {
        log.setAdditivity(true);
        FileAppender appender = new RollingFileAppender();
        PatternLayout layout = new PatternLayout();
        // log的输出形式
        String conversionPattern = "[%p] %d [%t] %c{3} (%F %L) - %m%n";
        layout.setConversionPattern(conversionPattern);
        appender.setLayout(layout);
        // log输出路径
        appender.setFile(logFileName == null ? DEFAULT_LOGFILE : logFileName);
        // log的文字码
        appender.setEncoding("UTF-8");
        // true:在已存在log文件后面追加 false:新log覆盖以前的log
        appender.setAppend(true);
        // 适用当前配置
        appender.activateOptions();

        // 将新的Appender加到Logger中
        Logger.getRootLogger().addAppender(appender);
        Logger.getLogger("DataNucleus").setLevel(Level.ERROR);
    }

    /**
     * print dpump usage
     */
    private static void printUsage() {
        log.info(String.format("\n"
                        + "DPump Usage : \n"
                        + "%-5s: %19s\n"
                        + "%-5s: %19s\n"
                        + "%-5s: %19s\n",
                " -f", "dpump config file",
                " -l", "optional, log file target",
                " -t", "optional, data time"));
    }


}
