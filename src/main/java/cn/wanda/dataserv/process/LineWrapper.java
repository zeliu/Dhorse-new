package cn.wanda.dataserv.process;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.java.Log;
import cn.wanda.dataserv.core.AssertUtils;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.process.processor.LineToJSONConfig;
import cn.wanda.dataserv.utils.JSONUtils;
import cn.wanda.dataserv.utils.StringUtil;
import cn.wanda.dataserv.utils.escape.JavaEscape;

@Log
@ToString
public class LineWrapper {

    private static int line_length_default = 256;

    private Line line;
    @Setter
    @Getter
    private int lineLength = line_length_default;
    private List<String> fieldsCache;
    @Setter
    @Getter
    private boolean splitted;
    @Setter
    @Getter
    private String fieldDelim;

    public void setFields(List<String> fields) {
        this.fieldsCache = fields;
        this.line = null;
        splitted = true;

    }

    public void setLine(Line line) {
        this.line = line;
        this.lineLength = (line.getLine().length() + 1) * 2;
        this.fieldsCache = null;
        splitted = false;
    }

    public void split(LineSchema schema) {
        if (!splitted) {
            String lineStr = line.getLine();
            AssertUtils.assertNotNull(lineStr, "line string cannot be null");

//			String[] fields = lineStr.split(this.fieldDelim);
//			fieldsCache = new ArrayList<String>(fields.length);
//			CollectionUtils.addAll(fieldsCache, fields);
            //this.fieldDelim = JavaEscape.unescapeJava(schema.getFieldDelim());
            //fieldsCache = Arrays.asList(lineStr.split(fieldDelim, -1));
            this.fieldDelim = schema.getFieldDelim();
            //this.fieldsCache = StringUtil.simpleSplit(lineStr, this.fieldDelim);
            fieldsCache = Arrays.asList(lineStr.split(JavaEscape.unescapeJava(schema.getFieldDelim()), -1));
            // release string line
            this.line = null;

            splitted = true;
        }
    }

    public void addField(int i, String[] s) {
        AssertUtils.assertTrue(splitted, "line must be splitted first");
        fieldsCache.addAll(i, Arrays.asList(s));
    }

    public void addField(int i, String s) {
        AssertUtils.assertTrue(splitted, "line must be splitted first");
        fieldsCache.add(i, s);
    }

    public void addField(String s) {
        AssertUtils.assertTrue(splitted, "line must be splitted first");
        fieldsCache.add(s);
    }

    public void removeField(int i) {
        AssertUtils.assertTrue(splitted, "line must be splitted first");
        fieldsCache.remove(i);
    }

    public String getField(int i) {
        AssertUtils.assertTrue(splitted, "line must be splitted first");
        return fieldsCache.get(i);
    }

    public void replaceField(int i, String v) {
        AssertUtils.assertTrue(splitted, "line must be splitted first");
        fieldsCache.set(i, v);
    }

    public Line getLine() {
        if (splitted) {
            String lineStr = StringUtil.join(fieldsCache, fieldDelim, lineLength);
            return new Line(lineStr);
        } else {
            return line;
        }
    }

    public LineWrapper newFrom() {
        LineWrapper l = new LineWrapper();
        l.setFieldDelim(this.fieldDelim);
        l.setLineLength(this.lineLength / 2);
        return l;
    }
    
    public void toJSONLine(LineToJSONConfig config,LineSchema schema) {
        if (splitted) {
        	if(config.getFieldIndex().size()!=config.getFieldNames().size()) {
                throw new LineToJSONException(
                        String.format("The size of field-name(%s) is not equals with the size of field-index(%s)!",
                                config.getFieldNames().size(), fieldsCache.size()));
            }
        	LinkedHashMap<String,String> map = new LinkedHashMap<>();
        	//try {
	        	if(config.getFieldIndex().size()>fieldsCache.size()) {
                    throw new LineToJSONException(
                            String.format("the size of fields(%s) is greater than the size of values(%s)!",
                                    config.getFieldNames().size(), fieldsCache.size()));
                }
        	/*} catch (Exception e) {
				System.out.println(this.getLine());
				e.printStackTrace();
			}*/
	        	/*List<Integer> index = config.getFieldIndex();
	        	Collections.sort(index);
	        	if(fieldsCache.size()<=index.get(index.size()-1))
	        		throw new LineToJSONException(
	        				String.format("The max of field-index(%s) is greater than the size of values(%s)!\nline:%s",
	        						index.get(index.size()-1),fieldsCache.simvnze(),this.getLine()));*/
        	//try {
				for(int i=0; i<config.getFieldIndex().size(); i++){
					String fieldName = config.getFieldNames().get(i);
					String fieldIndex = config.getFieldIndex().get(i);
					if(fieldName.indexOf(',')!=-1&&fieldName.lastIndexOf(',')!=fieldName.length()-1){
						String[] fieldNames = fieldName.split(",");
						String[] fieldIndices = fieldIndex.split(",");
						String value = fieldsCache.get(Integer.parseInt(fieldIndices[0]));
						if(StringUtils.isBlank(value)){
							value = fieldsCache.get(Integer.parseInt(fieldIndices[1]));
						}
						map.put(fieldNames[0], value);
					}else{
						if(!fieldsCache.get(Integer.parseInt(fieldIndex)).equals("\\N")){
							map.put(fieldName,fieldsCache.get(Integer.parseInt(fieldIndex)));
						}
					}
				}
			/*} catch (Exception e) {
				System.out.println(this.getLine());
				e.printStackTrace();
			}*/
            this.line = new Line(JSONUtils.toJSONString(map));
            this.splitted=false;
        }
    }
    
}
