package MRAppMaster;
import org.apache.hadoop.yarn.event.AbstractEvent;

import MRAppMaster.JobEventType;

public class JobEvent extends AbstractEvent<JobEventType>{
	private String jobId;
	public JobEvent(String jobID, JobEventType type)
	{
		super(type);
		this.jobId = jobID;
	}
	public String getJobId(){
		return jobId;
	}
}
