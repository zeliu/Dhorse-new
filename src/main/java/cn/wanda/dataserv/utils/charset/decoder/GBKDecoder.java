package cn.wanda.dataserv.utils.charset.decoder;//package cn.wanda.dataserv.utils.charset.decoder;
//
//import java.nio.charset.Charset;
//
///**
//*
//* 修改如下内容：decodeDouble方法实现
//* 当在进行双字节解码的时候，发现某一字节不在字符集指定范围内，原有编码方式为使用RPLACE_CHAR<br>
//* 现策略为抛弃第一个字符，单独使用第二个字符作为结果<br>
//* 主要解决GBK编码中混合出现其他编码格式时，会将后继一些分隔符等ascii字符吞并的现象
//*
//* @author songqian
//*
//*/
//public class GBKDecoder extends sun.nio.cs.ext.GBK.Decoder{
//
//	public GBKDecoder(Charset arg0) {
//		super(arg0);
//	}
//
//	@Override
//    protected char decodeDouble(int byte1, int byte2) {
//        if (((byte1 < 0) || (byte1 > 256))
//            || ((byte2 < start) || (byte2 > end)))
//            return decodeSingle(byte2);
//
//        return super.decodeDouble(byte1, byte2);
//    }
//}