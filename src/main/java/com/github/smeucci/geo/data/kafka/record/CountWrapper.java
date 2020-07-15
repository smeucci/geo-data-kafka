package com.github.smeucci.geo.data.kafka.record;

public class CountWrapper {

	private long count = 1;

	private boolean finalized;

	public CountWrapper plusOne() {
		this.count++;
		return this;
	}

	public long getCount() {
		return count;
	}

	public boolean isFinalized() {
		return finalized;
	}

	public void finalize() {
		this.finalized = true;
	}

	@Override
	public String toString() {
		return "CountWrapper [count=" + count + ", finalized=" + finalized + "]";
	}

}
