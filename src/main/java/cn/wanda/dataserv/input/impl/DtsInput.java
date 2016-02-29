package cn.wanda.dataserv.input.impl;//package cn.wanda.dataserv.input.impl;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.ArrayList;
//import java.util.Random;
//
//import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
//import cn.wanda.dataserv.config.location.DtsInputLocation;
//import cn.wanda.dataserv.core.Line;
//import cn.wanda.dataserv.engine.InputWorker;
//import cn.wanda.dataserv.input.AbstractInput;
//import cn.wanda.dataserv.input.InputException;
//import cn.wanda.dataserv.utils.charset.CharSetUtils;
//import lombok.extern.log4j.Log4j;
//
//import org.apache.commons.lang.StringUtils;
//
//import cn.wanda.dataserv.config.LocationConfig;
//import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
//import cn.wanda.dataserv.config.location.DtsInputLocation;
//import cn.wanda.dataserv.core.Line;
//import cn.wanda.dataserv.engine.InputWorker;
//import cn.wanda.dataserv.input.AbstractInput;
//import cn.wanda.dataserv.input.InputException;
//import cn.wanda.dataserv.utils.charset.CharSetUtils;
//
//@Log4j
//public class DtsInput extends AbstractInput {
//
//	private String item;
//
//	private String datatime;
//
//	private BufferedReader fr;
//
//	private Process p;
//
//	private String inputId;
//
//	@Override
//	public void init() {
//		//process params
//		LocationConfig l = this.inputConfig.getLocation();
//
//		if(!DtsInputLocation.class.isAssignableFrom(l.getClass())){
//			throw new InputException(
//			"input config type is not Dts!");
//		}
//
//		DtsInputLocation location = (DtsInputLocation) l;
//
//		this.inputId = this.inputConfig.getId();
//
//		this.item = ExpressionUtils.getOneValue(location.getDts().getItem());
//		this.datatime = ExpressionUtils.getOneValue(location.getDataTime());
//		if(StringUtils.isBlank(this.item)){
//			throw new InputException("item is empty.");
//		}
//		this.encoding = this.inputConfig.getEncode();
//
//		ArrayList<String> args = new ArrayList<String>();
//
//		args.add("sh");
//
//		args.add("script/dts.sh");
//
//		args.add(item);
//
//		args.add(datatime);
//
//		Random r = new Random();
//		r.setSeed(System.nanoTime());
//		String random = String.valueOf(r.nextInt(10000)) + String.valueOf(System.nanoTime()) + this.datatime;
//		args.add(random);
//
//		try {
//			p = Runtime.getRuntime().exec(args.toArray(new String[0]));
//			fr = new BufferedReader(new InputStreamReader(p.getInputStream(), CharSetUtils.getDecoderForName(this.encoding)));
//		} catch (IOException e) {
//			throw new InputException("I/O error occurs while init dts input.",e);
//		}
//	}
//
//	@Override
//	public Line readLine() {
//		try {
//			String line = fr.readLine();
//			if (StringUtils.isNotBlank(line)){
//				return new Line(line);
//			}else{
//				return Line.EOF;
//			}
//		} catch (IOException e) {
//			throw new InputException(e);
//		}
//	}
//
//	@Override
//	public void close() {
//		try {
//			if(fr != null){
//				fr.close();
//			}
//		} catch (IOException e) {
//			log.warn(String.format(
//						"Close file input failed:%s,%s", e.getMessage(),
//						e.getCause()));
//		}
//		BufferedReader reader = null;
//		int result = 0;
//		try {
//			reader = new BufferedReader(new InputStreamReader(p.getErrorStream(), this.encoding));
//			while(true){
//				String s = reader.readLine();
//				if(StringUtils.isBlank(s)){
//					break;
//				}
//				if(s.startsWith("FATAL")){
//					log.error(s);
//					result = 1;
//				}else{
//					log.info(s);
//				}
//			}
//		} catch (IOException e) {
//			log.warn("read dts process info error.", e);
//		} finally{
//			if(reader != null){
//				try {
//					reader.close();
//				} catch (IOException e) {
//					log.warn(String.format(
//							"Close file input failed:%s,%s", e.getMessage(),
//							e.getCause()));
//				}
//			}
//		}
//
//		if (null != p) {
//			while (true) {
//				try {
//					result += p.waitFor();
//				} catch (InterruptedException ie) {
//					// interrupted; loop around.
//					continue;
//				}
//
//				break;
//			}
//		}
//		if(result != 0){
//			InputWorker.INPUT_ERROR.put(this.inputId + "_close", new InputException("dts input failed."));
//		}
//	}
//
//}