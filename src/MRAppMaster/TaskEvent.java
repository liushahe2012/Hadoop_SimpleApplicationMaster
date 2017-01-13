package MRAppMaster;

import org.apache.hadoop.yarn.event.AbstractEvent;

import MRAppMaster.TaskEventType;

public class TaskEvent extends AbstractEvent<TaskEventType>{
	private String taskID;
	public TaskEvent(String taskId, TaskEventType type)
	{
		super(type);
		this.taskID = taskId;
	}
	
	public String getTaskID(){
		return taskID;
	}
}
