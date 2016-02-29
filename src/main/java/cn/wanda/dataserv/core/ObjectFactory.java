package cn.wanda.dataserv.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cn.wanda.dataserv.config.ConfigTypeVals;
import cn.wanda.dataserv.config.InputConfig;
import cn.wanda.dataserv.config.OutputConfig;
import cn.wanda.dataserv.config.RuntimeConfig;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.input.impl.JDBCInput;
import cn.wanda.dataserv.monitor.MonitorStorageManager;
import cn.wanda.dataserv.output.Output;
import cn.wanda.dataserv.process.Processor;
import cn.wanda.dataserv.process.processor.*;
import cn.wanda.dataserv.storage.Storage;
import cn.wanda.dataserv.storage.StorageManager;
import cn.wanda.dataserv.storage.StorageManagerDecorator;
import cn.wanda.dataserv.storage.mem.DefaultStorageManager;
import cn.wanda.dataserv.storage.ram.FastStorage;
import cn.wanda.dataserv.utils.DPClassLoader;
import lombok.extern.log4j.Log4j;


@Log4j
public class ObjectFactory {
    private static ObjectFactory instance = new ObjectFactory();

    private static final Map<String, Class<? extends Storage>> storagemap = new HashMap<String, Class<? extends Storage>>();

    private static final Map<Class, Class<? extends Processor>> processorClassMap = new HashMap<Class, Class<? extends Processor>>();

    private static final Map<String, Class<? extends ConfigLoader>> configLoaderClassMap = new HashMap<String, Class<? extends ConfigLoader>>();

    private static final Map<String, Set<String>> LOAD_JAR_LIST = new HashMap<String, Set<String>>();

    public static final Map<String, ClassLoader> dpumpClassLoader = new HashMap<String, ClassLoader>();

    private static Properties properties = null;

    private static String CONFIG_FILE = "loadjarlist.properties";

    static {
        storagemap.put("mem", FastStorage.class);
        processorClassMap.put(ConsoleConfig.class, ConsoleProcessor.class);
        processorClassMap.put(AddFieldConfig.class, AddField.class);
        processorClassMap.put(FilterFieldConfig.class, FilterField.class);
        processorClassMap.put(RemoveFieldConfig.class, RemoveField.class);
        processorClassMap.put(FilterRowConfig.class, FilterRow.class);
        processorClassMap.put(ReplaceFieldConfig.class, ReplaceField.class);
        processorClassMap.put(LineToJSONConfig.class, LineToJSON.class);
        processorClassMap.put(ReplaceFieldDelimitConfig.class, ReplaceFieldDelimit.class);

        configLoaderClassMap.put("yaml", YamlConfigLoader.class);
        configLoaderClassMap.put("json", JsonConfigLoader.class);
        try {
            loadProperties(CONFIG_FILE);

//			// HDP jar set
//			String hdpJarString = properties.getProperty("hdp");
//			loadJarSet(HdpOutput.class.getName(), hdpJarString);
//
//			// hive
//			String hdpHiveJarString = properties.getProperty("hdphive");
//			loadJarSet(HdpHqlInput.class.getName(), hdpHiveJarString);
//			loadJarSet(HdpHiveOutput.class.getName(), hdpHiveJarString);
//
//			// hive2
//			String hive2JarString = properties.getProperty("hive2");
//			loadJarSet(Hql2Input.class.getName(), hive2JarString);
//			loadJarSet(Hive2Output.class.getName(), hive2JarString);

            //jdbc
            loadjdbc(JDBCInput.class.getName());

        } catch (IOException e) {
            log.error("load jar failed.", e);
        }

    }

    private ObjectFactory() {
        // nothing
    }

    public static ObjectFactory getInstance() {
        return instance;
    }

    private static void loadJars(Class<?> clz, Set<String> jarSet) {
        if (jarSet == null || jarSet.size() == 0 || dpumpClassLoader.get(clz.getName()) != null) {
            return;
        }
        List<URL> urlList = new ArrayList<URL>();
        for (String f : jarSet) {
            File file = new File(f);
            try {
                urlList.add(file.toURI().toURL());
            } catch (MalformedURLException e) {
                log.debug(e.getMessage(), e);
                log.info(String.format("load jar: %s failed!", f));
            }
        }

        DPClassLoader hdpCL = new DPClassLoader(urlList.toArray(new URL[urlList.size()]), ObjectFactory.class.getClassLoader());
        dpumpClassLoader.put(clz.getName(), hdpCL);
    }

    private static void loadProperties(String fileName) throws IOException {
        properties = new Properties();

        InputStream is = null;
        try {
            is = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(fileName);
            properties.load(is);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (Exception e) {
                log.error("关闭属性配置文件失败");
            }
        }
    }

    private static void loadJarSet(String className, String jarSetString) {

        String[] jarSetArray = jarSetString.split(";");
        if (jarSetArray != null) {
            Set<String> jarSet = new HashSet<String>();
            for (String jarName : jarSetArray) {
                jarSet.add("pluginlib/" + jarName);
            }
            log.info("init jar set: " + className + " set:" + jarSet);
            LOAD_JAR_LIST.put(className, jarSet);
        }
    }

    private static void loadjdbc(String className) {
        String homeFile = System.getenv("DHORSE_HOME");
        if (homeFile == null) {
            System.err.println("Warning: DHORSE_HOME not set, using current directory");
            homeFile = System.getProperty("user.dir");
        }
        File home = new File(homeFile);
        File[] libs = new File(home + "/pluginlib/jdbc").listFiles();
        Set<String> jarSet = new HashSet<String>();
        if (libs != null) {
            for (File file : libs) {
                if (file.getName().toLowerCase().endsWith(".jar")) {
                    jarSet.add(file.getAbsolutePath());
                }
            }
        }
        log.info("init jar set: " + className + " set:" + jarSet);
        LOAD_JAR_LIST.put(className, jarSet);
    }

    public Storage createStorage(RuntimeConfig storageConfig, String id) {

        String storageName = storageConfig.getStorageName();

        Class<? extends Storage> storageClazz;
        try {
            storageClazz = storagemap.get(storageName);
            if (storageClazz == null) {
                throw new IllegalArgumentException("Storage:" + storageName
                        + "not found!");
            }
            Storage storage = storageClazz.newInstance();
            if (!storage.init(storageConfig, id)) {
                throw new IllegalStateException("init storage failed! ");
            }
            return storage;
        } catch (InstantiationException e) {
            log.error("init Storage:" + storageName + "failed!");
            throw new IllegalStateException(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            log.error("init Storage:" + storageName + "failed!");
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public StorageManager createStorageManager(RuntimeConfig storageConfig,
                                               List<Object> inputProcessor,
                                               List<OutputProcessorConfig> outputProcessor) {
        // TODO: config
        List<Processor> inputProcessorList = new ArrayList<Processor>();
        Map<String, List<Processor>> outputProcessorMap = new HashMap<String, List<Processor>>();
        try {
            if (inputProcessor != null)
                for (Object p : inputProcessor) {
                    Processor pr = processorClassMap.get(p.getClass())
                            .newInstance();
                    pr.setConfig(p);
                    inputProcessorList.add(pr);
                }
            if (outputProcessor != null)
                for (OutputProcessorConfig p : outputProcessor) {
                    Processor pr = processorClassMap.get(p.getProcessorConfig().getClass()).newInstance();
                    pr.setConfig(p.getProcessorConfig());
                    String[] keys = p.getId().split(",");
                    for (String key : keys) {
                        List<Processor> outputProcessorList = outputProcessorMap.get(key.trim());
                        if (outputProcessorList == null) {
                            outputProcessorList = new ArrayList<Processor>();
                            outputProcessorMap.put(key.trim(), outputProcessorList);
                        }
                        outputProcessorList.add(pr);
                    }
                }
        } catch (Exception e) {
            log.error("init StorageManager failed!");
            throw new IllegalStateException(e.getMessage(), e);
        }
        StorageManager result = new DefaultStorageManager();
        if (storageConfig.getMonitorPeriod() > 0) {
            result = new MonitorStorageManager(result, storageConfig.getMonitorPeriod());
        }
        return new StorageManagerDecorator(result,
                inputProcessorList, outputProcessorMap);
    }

    public Input createInput(InputConfig inputConfig, RuntimeConfig runtime) {

        ConfigTypeVals c = ConfigTypeVals.getByInputLocation(inputConfig
                .getLocation().getClass());
        Class<? extends Input> inputClazz = null;
        try {
            loadJars(c.inputClass, LOAD_JAR_LIST.get(c.inputClass.getName()));
            ClassLoader cl = dpumpClassLoader.get(c.inputClass.getName());
            log.info("get a class loader for class:" + c.inputClass.getName() + " " + cl);
            if (cl == null) {
                cl = ObjectFactory.class.getClassLoader();
            }
            inputClazz = (Class<? extends Input>) cl.loadClass(c.inputClass.getName());
        } catch (ClassNotFoundException e1) {
            log.error("init Input failed!", e1);
        }
        Input input;
        try {
            input = inputClazz.newInstance();
            input.setConfig(inputConfig, runtime);
            return input;
        } catch (InstantiationException e) {
            log.error("init Input failed!");
            throw new IllegalStateException(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            log.error("init Input failed!");
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public Output createOutput(OutputConfig outputConfig, RuntimeConfig runtime) {

        ConfigTypeVals c = ConfigTypeVals.getByOutputLocation(outputConfig
                .getLocation().getClass());
        Class<? extends Output> outputClazz = null;
        try {
            loadJars(c.outputClass, LOAD_JAR_LIST.get(c.outputClass.getName()));
            ClassLoader cl = dpumpClassLoader.get(c.outputClass.getName());
            log.info("get a class loader for class:" + c.outputClass.getName() + " " + cl);
            if (cl == null) {
                cl = ObjectFactory.class.getClassLoader();
            }
            outputClazz = (Class<? extends Output>) cl.loadClass(c.outputClass.getName());
        } catch (ClassNotFoundException e1) {
            log.error("init Output failed!", e1);
        }
        Output output;
        try {
            output = outputClazz.newInstance();
            output.setConfig(outputConfig, runtime);
            return output;
        } catch (InstantiationException e) {
            log.error("init Input failed!");
            throw new IllegalStateException(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            log.error("init Input failed!");
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public ConfigLoader createConfigLoader(String filePath) {

        String extention = filePath.substring(filePath.lastIndexOf('.') + 1);

        Class<? extends ConfigLoader> configLoaderClazz = configLoaderClassMap.get(extention);

        if (configLoaderClazz == null) {
            configLoaderClazz = YamlConfigLoader.class;
        }
        ConfigLoader configLoader;
        try {
            configLoader = configLoaderClazz.newInstance();
        } catch (InstantiationException e) {
            log.error("init ConfigLoader failed!");
            throw new IllegalStateException(e.getMessage(), e);
        } catch (IllegalAccessException e) {
            log.error("init ConfigLoader failed!");
            throw new IllegalStateException(e.getMessage(), e);
        }

        return configLoader;
    }
}
