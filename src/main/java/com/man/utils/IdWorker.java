package com.man.utils;

public class IdWorker {

	private  long workerId;
	private long sequence = 0L;
	public static final long maxWorkerId = 63L;
	public static final long sequenceMask = 1023L;
	private long lastTimestamp = -1L;

	public IdWorker(Long workerId) {
		if ((workerId > 63L) || (workerId < 0L)) {
			throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0",
					new Object[] { Long.valueOf(63L) }));
		}
		this.workerId = workerId;
	}
	
	
	public IdWorker(){}

	public synchronized Long nextId() {
		long timestamp = timeGen();
		if (this.lastTimestamp == timestamp) {
			this.sequence = (this.sequence + 1L & 0x3FF);
			if (this.sequence == 0L) {
				timestamp = tilNextMillis(this.lastTimestamp);
			}
		} else {
			this.sequence = 0L;
		}
		if (timestamp < this.lastTimestamp) {
			try {
				throw new Exception(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds",
						new Object[] { Long.valueOf(this.lastTimestamp - timestamp) }));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		this.lastTimestamp = timestamp;
		long nextId = timestamp - 1456714605277L << 16 | this.workerId << 10 | this.sequence;

		return nextId;
	}

	protected Long tilNextMillis(Long lastTimestamp) {
		Long timestamp = timeGen();
		while (timestamp <= lastTimestamp) {
			timestamp = timeGen();
		}
		return timestamp;
	}

	protected Long timeGen() {
		return System.currentTimeMillis();
	}

	public static void main(String[] args) {

	}
}
