package cn.wanda.dataserv.core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

import lombok.extern.log4j.Log4j;

import org.yaml.snakeyaml.Yaml;

@Log4j
public class YamlConfigLoader implements ConfigLoader {

    @Override
    public Map load(String filePath) {
        Yaml y = new Yaml();
        Map content = null;
        try {
            content = (Map) y.load(new FileInputStream(filePath));
        } catch (FileNotFoundException e) {
            log.error(e.getMessage(), e);
        }
        return content;
    }

}