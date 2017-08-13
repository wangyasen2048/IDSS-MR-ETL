
package com.cucrz.idss.hadoop.etl.cache;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cucrz.idss.hadoop.etl.exception.IdsshvException;

/**
 * @author hadoop
 * 
 */
public class Caches {
	public static void createRegionMarketCache(Configuration conf, FileSystem fs) throws Exception {
		DistributedCache.createSymlink(conf);
		Path rm = new Path(conf.get("user.cache.regMarket"));
		if (!fs.exists(rm)) {
			throw new IdsshvException("没有数据:" + rm.toString());
		}
		DistributedCache.addCacheFile(new URI(rm.toUri() + "#regionMarket"), conf);
	}
	
	public static void createRegionChannelCache(Configuration conf, FileSystem fs) throws Exception {
		DistributedCache.createSymlink(conf);
		Path rm = new Path(conf.get("user.cache.regChannel"));
		if (!fs.exists(rm)) {
			throw new IdsshvException("没有数据:" + rm.toString());
		}
		DistributedCache.addCacheFile(new URI(rm.toUri() + "#regionChannel"), conf);
	}
	
	public static void createProTimeCache(Configuration conf, FileSystem fs) throws Exception {
		DistributedCache.createSymlink(conf);
		Path rm = new Path(conf.get("user.cache.proTime"));
		if (fs.exists(rm)) {
			DistributedCache.addCacheFile(new URI(rm.toUri() + "#proTime"), conf);
		}
	}

	/**
	 * @param conf
	 * @param fs
	 * @throws Exception 
	 */
	public static void createVodProgramCache(Configuration conf, FileSystem fs) throws Exception {
		DistributedCache.createSymlink(conf);
		Path rm = new Path(conf.get("user.cache.vodProgram"));
		if (!fs.exists(rm)) {
			throw new IdsshvException("没有数据:" + rm.toString());
		}
		DistributedCache.addCacheFile(new URI(rm.toUri() + "#vodProgram"), conf);
	}
}
