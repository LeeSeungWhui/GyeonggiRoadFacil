package com.ybs.pullapidata.gyeonggiroadfacil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.ybs.pullapidata.gyeonggiroadfacil.ApiConnection;
import com.ybs.pullapidata.gyeonggiroadfacil.DbConnection;

public class GyeonggiRoadFacil 
{
	static public ApiConnection apiconnection;
	static public String BaseDate, BaseTime;
	static public String tableName = "GYEONGGI_ROAD_FACIL";
	
	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException 
	{
		// 기본 설정
		long time = System.currentTimeMillis();
		SimpleDateFormat _date = new SimpleDateFormat("YYYYMMdd");
		SimpleDateFormat _time = new SimpleDateFormat("HHmmss");
		BaseDate = _date.format(new Date(time));
		BaseTime = _time.format(new Date( time));
		List<String> column = new ArrayList<String>();
		column.add("seq");
		column.add("SUM_YY");
		column.add("SIGUN_NM");
		column.add("SIGUN_CD");
		column.add("FACLTS_NM");
		column.add("FACLTS_DIV_NM");
		column.add("FACLTS_LOC");
		column.add("FACLTS_COMPLTN_YY");
		column.add("ASORTMT_DIV_NM");
		column.add("ROUTE_NM");
		
		// DB 연결
		String host = "192.168.0.53";
		String name = "HVI_DB";
		String user = "root";
		String pass = "dlatl#001";
		DbConnection dbconnection = new DbConnection(host, name, user, pass);
	    dbconnection.Connect();
	    String sql = "";
	    
	    // sequence 받아오기
	    int seq = 1;
		try {
			sql = "Select max(SEQ) as M from " + tableName;
			dbconnection.runQuery(sql);
			dbconnection.getResult().next();
			seq = dbconnection.getResult().getInt("M") + 1;
		} catch (Exception e) {
			// TODO Auto-generated catch block
		}
		
	    // api data 받아서 csv파일 생성
	    String FileName = tableName + "_" + BaseDate + BaseTime + ".csv";
	    BufferedWriter bufWriter = new BufferedWriter(new FileWriter(FileName));
	    CreateCSV(bufWriter, column);
	    apiconnection = new ApiConnection();
    	try 
    	{
    		apiconnection.setUrl("https://openapi.gg.go.kr/RoadFacilities");
			apiconnection.setServiceKey("KEY", "78fbc86168bc47d1a3f1f7c540dd1032");
			apiconnection.pullData();
			System.out.println(apiconnection.urlBuilder);
			apiconnection.setRow("row");
			List<List<String>> data = new ArrayList<List<String>>();
			data.add(new ArrayList<String>());
			for(int i = 1; i < column.size(); i++)
			{
				data.add(apiconnection.getAttributeValueFromRow(column.get(i)));
			}		
			for(int i = 0; i < data.get(1).size(); i++, seq++)
			{
				data.get(0).add(String.valueOf(seq));
			}
			WriteCSV(bufWriter, data); 
		} catch (Exception e) 
    	{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    bufWriter.close();
	    
	    // DB에 입력
	    sql = "LOAD DATA LOCAL INFILE '" + FileName + "' INTO TABLE " + tableName + " FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES";
	    dbconnection.LoadLocalData(sql);
	}
	
	public static void CreateCSV(BufferedWriter bufWriter, List<String> Column)
	{
		try
		{
			int i = 0;
			for(; i < Column.size() - 1; i++)
			{
				bufWriter.write("\"" + Column.get(i) + "\",");
			}
			bufWriter.write("\"" + Column.get(i) + "\"");
			bufWriter.newLine();
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void WriteCSV(BufferedWriter bufWriter, List<List<String>> datalist) throws IOException
	{
//		System.out.println(datalist.get(0).size() + " " + datalist.get(1).size()+ " " + datalist.get(2).size()+ " " + datalist.get(3).size()+ " " + datalist.get(4).size()+ " " + datalist.get(5).size()+ " " + datalist.get(6).size());
		String buffer = "";
		for(int i = 0; i < datalist.get(0).size(); i++)
		{
			int j = 0;
			for(; j < datalist.size() - 1; j++)
			{
				if(datalist.get(j).size() > i && datalist.get(j).get(i).contains("</"))
				{
					buffer += "\"" + datalist.get(j).get(i).substring(0,datalist.get(j).get(i).indexOf('<') ) + "\",";
				}
				else
				{
					buffer += "\"" + datalist.get(j).get(i) + "\",";
				}
			}
			if(datalist.get(j).size() > i && datalist.get(j).get(i).contains("</"))
			{
				buffer += "\"" + datalist.get(j).get(i).substring(0,datalist.get(j).get(i).indexOf('<') );
			}
			else
			{
				buffer += "\"" + datalist.get(j).get(i);
			}
			buffer += "\"\n";
		}
		System.out.print(buffer);
		bufWriter.write(buffer);
	}
}
