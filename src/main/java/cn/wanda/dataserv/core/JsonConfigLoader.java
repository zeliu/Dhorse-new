package cn.wanda.dataserv.core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Map;

import lombok.extern.log4j.Log4j;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

@Log4j
public class JsonConfigLoader implements ConfigLoader {

    @Override
    public Map load(String filePath) {
        Gson gson = new Gson();
        try {
            Map content = gson.fromJson(new InputStreamReader(new FileInputStream(filePath)), Map.class);
            return content;
        } catch (JsonSyntaxException e) {
            log.error(e.getMessage(), e);
        } catch (JsonIOException e) {
            log.error(e.getMessage(), e);
        } catch (FileNotFoundException e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

}