package com.cucrz.idss.hadoop.etl.synchronizeBaseData.bean;

import java.util.Date;

import com.cucrz.idss.hadoop.etl.util.DateUtil;

public class channelBean {

	private String channel_code;
	private String region_channelname;
	private String netoperator_id;
	private String service_id;
	private String ts_id;
	private String net_id;
	private String create_time;
	private String modified_time = DateUtil.DATE_TIME_FORMATER
			.format(new Date());;

	public channelBean(String channel_code,
			String region_channelname, String netoperator_id,
			String service_id, String ts_id, String net_id, String create_time,
			String modified_time) {
		super();

		this.channel_code = channel_code;
		this.region_channelname = region_channelname;
		this.netoperator_id = netoperator_id;
		this.service_id = service_id;
		this.ts_id = ts_id;
		this.net_id = net_id;
		this.create_time = create_time;
		this.modified_time = modified_time;
	}

	public String getChannel_code() {
		return channel_code;
	}

	public void setChannel_code(String channel_code) {
		this.channel_code = channel_code;
	}

	public String getRegion_channelname() {
		return region_channelname;
	}

	public void setRegion_channelname(String region_channelname) {
		this.region_channelname = region_channelname;
	}

	public String getNetoperator_id() {
		return netoperator_id;
	}

	public void setNetoperator_id(String netoperator_id) {
		this.netoperator_id = netoperator_id;
	}

	public String getService_id() {
		return service_id;
	}

	public void setService_id(String service_id) {
		this.service_id = service_id;
	}

	public String getTs_id() {
		return ts_id;
	}

	public void setTs_id(String ts_id) {
		this.ts_id = ts_id;
	}

	public String getNet_id() {
		return net_id;
	}

	public void setNet_id(String net_id) {
		this.net_id = net_id;
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

	public String toString() {
		return channel_code + "," + region_channelname + ","
				+ netoperator_id + "," + service_id + "," + ts_id + ","
				+ net_id + "," + create_time + "," + modified_time;
	}

	@Override
	public boolean equals(Object obj) {
		channelBean channel = (channelBean) obj;
		return channel_code.equals(channel.channel_code)
				&& netoperator_id.equals(channel.netoperator_id)
				&& service_id.equals(channel.service_id);
	}

	@Override
	public int hashCode() {
		String in = channel_code + service_id +netoperator_id ;
		return in.hashCode();
	}

}
