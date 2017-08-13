package com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean;

import java.util.Date;

import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class videoBean {

	private String video_id;
	private String video_code;
	private String video_name;
	private String video_create_time;
	private String netoperator_id;
	private String create_time;
	private String modified_time = DateUtil.DATE_TIME_FORMATER
			.format(new Date());

	@Override
	public String toString() {
		return video_id + "," + video_code + "," + video_name + ","
				+ video_create_time + "," + netoperator_id + "," + create_time
				+ "," + modified_time;
	}

	public videoBean(String video_id, String video_code, String video_name,
			String video_create_time, String netoperator_id,
			String create_time, String modified_time) {
		super();
		this.video_id = video_id;
		this.video_code = video_code;
		this.video_name = video_name;
		this.video_create_time = video_create_time;
		this.netoperator_id = netoperator_id;
		this.create_time = create_time;
		this.modified_time = modified_time;
	}

	public String getVideo_id() {
		return video_id;
	}

	public void setVideo_id(String video_id) {
		this.video_id = video_id;
	}

	public String getVideo_code() {
		return video_code;
	}

	public void setVideo_code(String video_code) {
		this.video_code = video_code;
	}

	public String getVideo_name() {
		return video_name;
	}

	public void setVideo_name(String video_name) {
		this.video_name = video_name;
	}

	public String getVideo_create_time() {
		return video_create_time;
	}

	public void setVideo_create_time(String video_create_time) {
		this.video_create_time = video_create_time;
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
		videoBean video = (videoBean) obj;
		return video_id.equals(video.video_id)
				&& video_name.equals(video.video_name);
	}

	@Override
	public int hashCode() {
		String in = video_id + video_name;
		return in.hashCode();
	}

}
