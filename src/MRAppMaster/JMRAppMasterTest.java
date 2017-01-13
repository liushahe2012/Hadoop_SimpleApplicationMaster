package MRAppMaster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import MRAppMaster.JobEvent;
import MRAppMaster.JobEventType;

public class JMRAppMasterTest {
    @SuppressWarnings({ "unchecked", "resource" })
    public static void main(String[] args) {
        String jobID = "job_20170112_11";
        JMRAppMaster appMaster = new JMRAppMaster("Simple MRAppMaster Test", jobID, 10);
        YarnConfiguration conf = new YarnConfiguration(new Configuration());
        try {
            appMaster.serviceInit(conf);
            appMaster.serviceStart();
        } catch (Exception e) {
            e.printStackTrace();
        }
        appMaster.getDispatcher().getEventHandler().handle(new JobEvent(jobID, JobEventType.JOB_KILL));
        appMaster.getDispatcher().getEventHandler().handle(new JobEvent(jobID, JobEventType.JOB_INIT));
    }
}