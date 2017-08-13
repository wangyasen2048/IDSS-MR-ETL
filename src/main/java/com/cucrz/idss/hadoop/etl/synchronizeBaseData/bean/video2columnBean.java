package com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean;

import java.util.Date;

import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class video2columnBean {
	private String column_id;
	private String column_name;
	private String video_id;
	private String video_name;
	private String netoperator_id;
	private String create_time;
	private String modified_time = DateUtil.DATE_TIME_FORMATER
			.format(new Date());

	@Override
	public String toString() {
		return column_id + "," + column_name + "," + video_id + ","
				+ video_name + "," + netoperator_id + "," + create_time + ","
				+ modified_time;
	}

	public video2columnBean(String column_id, String column_name,
			String video_id, String video_name, String netoperator_id,
			String create_time, String modified_time) {
		super();
		this.column_id = column_id;
		this.column_name = column_name;
		this.video_id = video_id;
		this.video_name = video_name;
		this.netoperator_id = netoperator_id;
		this.create_time = create_time;
		this.modified_time = modified_time;
	}

	public String getColumn_id() {
		return column_id;
	}

	public void setColumn_id(String column_id) {
		this.column_id = column_id;
	}

	public String getColumn_name() {
		return column_name;
	}

	public void setColumn_name(String column_name) {
		this.column_name = column_name;
	}

	public String getVideo_id() {
		return video_id;
	}

	public void setVideo_id(String video_id) {
		this.video_id = video_id;
	}

	public String getVideo_name() {
		return video_name;
	}

	public void setVideo_name(String video_name) {
		this.video_name = video_name;
	}

	public String getNetoperator_id() {
		return netoperator_id;
	}

	public void setNetoperator_id(String netoperator_id) {
		this.netoperator_id = netoperator_id;
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
		video2columnBean video2cloumn = (video2columnBean) obj;
		return column_id.equals(video2cloumn.column_id)
				&& video_id.equals(video2cloumn.video_id);
	}

	@Override
	public int hashCode() {
		String in = video_id + column_id;
		return in.hashCode();
	}

}
