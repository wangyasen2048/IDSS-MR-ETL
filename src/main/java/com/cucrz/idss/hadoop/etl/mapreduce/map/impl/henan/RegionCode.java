package com.cucrz.idss.hadoop.etl.mapreduce.map.impl.henan;

/**
 * 河南的区域名称  > code的对照
 * @author qianzhiqin
 * @creatTime 2015年4月21日 18:14
 */
public enum RegionCode {
	
	any("410500"),
	heb("410600"),
	jiaoz("410800"),
	jiy("419001"),
	kaif("410200"),
	luoh("411100"),
	luoy("410300"),
	nany("411300"),
	pds("410400"),
	puy("410900"),
	shangq("411400"),
	smx("411200"),
	xinx("410700"),
	xiny("411500"),
	xuc("411000"),
	zhengzh("410100"),
	zhouk("411600"),
	zmd("411700");
	
	private final String code;
	
	RegionCode(String code){
		this.code = code;
	}
	public String getCode() {
        return code;
    }
}
