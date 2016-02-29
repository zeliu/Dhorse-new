package cn.wanda.dataserv.config.location;//package cn.wanda.dataserv.config.location;
//
//import lombok.Data;
//
//import cn.wanda.dataserv.config.LocationConfig;
//import cn.wanda.dataserv.config.annotation.Config;
//import cn.wanda.dataserv.config.annotation.RequiredType;
//import cn.wanda.dataserv.config.el.Expression;
//import cn.wanda.dataserv.config.resource.DtsConf;
//
//@Data
//public class DtsInputLocation extends LocationConfig {
//
//	@Config(name = "dts")
//	private DtsConf dts;
//
//	@Config(name = "data-time", require=RequiredType.OPTIONAL)
//	private Expression dataTime;
//}