package cn.wanda.dataserv.output.impl;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.location.EXCELOutputLocation;
import cn.wanda.dataserv.config.resource.EXCELConf;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.output.AbstractOutput;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * Created by liuze on 2015/10/30 0030.
 */
public class ExcelOutput extends AbstractOutput {

    EXCELConf excelConf;


    EXCELOutputLocation location;

    protected String templeteFile;

    protected String outputFile;

    protected String sheetName;

    protected String columnType;

    protected String fieldDelim;


    protected int rowNum;

    protected XSSFWorkbook workbook;

    protected XSSFSheet worksheet;

    protected FileOutputStream fileOutputStream;

    protected String[] columnTypeArr;

    @Override
    public void writeLine(Line line) {

        String[] str = line.getLine().split(this.fieldDelim);
        XSSFRow sumrow=null;
        if (worksheet.getRow(rowNum)==null){
            sumrow = worksheet.createRow(rowNum);
        }else{
            sumrow=worksheet.getRow(rowNum);
        }

        for(int j=0;j<str.length;j++){

            XSSFCell cell=null;
            if (sumrow.getCell(j) == null){
                cell = sumrow.createCell(j);
            }else{
                cell=sumrow.getCell(j);
            }
            if(columnTypeArr[j].equals("int"))
            {
                cell.setCellValue(Double.parseDouble(str[j]));
            }else{
                cell.setCellValue(str[j]);

            }
        }

        rowNum++;

    }

    @Override
    public void close() {

        if (fileOutputStream != null) {
            try {
                fileOutputStream.close();

            }  catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    @Override
    public void last(boolean success) {
        if(success){

            try {
                this.fileOutputStream = new FileOutputStream(outputFile);
                workbook.write(fileOutputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }


        }
    }

    @Override
    public void init() {

        // process params
        LocationConfig l = this.outputConfig.getLocation();

        if (!EXCELOutputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("output config type is not EXCEL!");
        }

        location = (EXCELOutputLocation) l;
        excelConf = location.getExcelConf();


        this.templeteFile = excelConf.getTempleteFile();
        this.outputFile = excelConf.getOutputFile();
        this.sheetName = excelConf.getSheetName();
        this.rowNum = Integer.parseInt(excelConf.getRowNum())-1;
        this.columnType = excelConf.getColumnType();
        this.fieldDelim = excelConf.getFieldDelim();
        this.columnTypeArr=columnType.trim().split(",");
        try {
            this.workbook = new XSSFWorkbook ( new FileInputStream(templeteFile)) ;
            this.worksheet =workbook.getSheet(sheetName);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }









}
