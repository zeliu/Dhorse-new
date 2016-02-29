package cn.wanda.dataserv.utils;

import java.io.IOException;

import lombok.extern.log4j.Log4j;
import parquet.org.codehaus.jackson.map.ObjectMapper;

@Log4j
public class JSONUtils {

	private final static ObjectMapper mapper = new ObjectMapper();
	
	public static String toJSONString(Object obj){
		try {
			String jsonStr = mapper.writeValueAsString(obj);
			log.debug("object convert to :" + jsonStr);
			return jsonStr;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}
}
