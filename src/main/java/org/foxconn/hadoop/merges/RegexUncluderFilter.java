package org.foxconn.hadoop.merges;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexUncluderFilter implements PathFilter{
	private final String regex;
	public RegexUncluderFilter(String regex) {
		this.regex=regex;
	}
	@Override
	public boolean accept(Path path) {
		boolean flag = path.toString().matches(regex);
		return !flag;
	}

}
