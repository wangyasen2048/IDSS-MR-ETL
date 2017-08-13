package com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean;

import java.util.Date;

import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class columnBean {

	private String column_id;
	private String netoperator_id;
	private String column_code;
	private String parent_code;
	private String column_name;
	private int level;
	private String path;
	private int order;
	private String market_id;
	private String create_time;
	private String modified_time = DateUtil.DATE_TIME_FORMATER
			.format(new Date());

	@Override
	public String toString() {
		return column_id + "," + netoperator_id + "," + column_code + ","
				+ parent_code + "," + column_name + "," + level + "," + path
				+ "," + order + "," + market_id + "," + create_time + ","
				+ modified_time;
	}

	public columnBean(String column_id, String netoperator_id,
			String column_code, String parent_code, String column_name,
			int level, String path, int order, String market_id,
			String create_time, String modified_time) {
		super();
		this.column_id = column_id;
		this.netoperator_id = netoperator_id;
		this.column_code = column_code;
		this.parent_code = parent_code;
		this.column_name = column_name;
		this.level = level;
		this.path = path;
		this.order = order;
		this.market_id = market_id;
		this.create_time = create_time;
		this.modified_time = modified_time;
	}

	public String getColumn_id() {
		return column_id;
	}

	public void setColumn_id(String column_id) {
		this.column_id = column_id;
	}

	public String getNetoperator_id() {
		return netoperator_id;
	}

	public void setNetoperator_id(String netoperator_id) {
		this.netoperator_id = netoperator_id;
	}

	public String getColumn_code() {
		return column_code;
	}

	public void setColumn_code(String column_code) {
		this.column_code = column_code;
	}

	public String getParent_code() {
		return parent_code;
	}

	public void setParent_code(String parent_code) {
		this.parent_code = parent_code;
	}

	public String getColumn_name() {
		return column_name;
	}

	public void setColumn_name(String column_name) {
		this.column_name = column_name;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public String getMarket_id() {
		return market_id;
	}

	public void setMarket_id(String market_id) {
		this.market_id = market_id;
	}

	public String getCreate_time() {
		return create_time;
	}

	public void setCreate_time(String create_time) {
		this.create_time = create_time;
	}

	public String getModified_time() {
		return modified_time;
	}

	public void setModified_time(String modified_time) {
		this.modified_time = modified_time;
	}

	@Override
	public boolean equals(Object obj) {
		columnBean cloumn = (columnBean) obj;
		return column_id.equals(cloumn.column_id)
				&& column_name.equals(cloumn.column_name) &&market_id.equals(cloumn.market_id);
	}

	@Override
	public int hashCode() {
		String in = column_id + column_name;
		return in.hashCode();
	}
}
