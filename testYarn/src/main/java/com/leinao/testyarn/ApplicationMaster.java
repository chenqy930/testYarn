package com.leinao.testyarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("all")
public class ApplicationMaster {
	private Configuration conf;
	private AMRMClientAsync amRMClient;
	private NMClientAsync nmClient;
	private NMCallbackHandler containerListener;
	private String appMasterHostname = "";
	private int appMasterRpcPort = -1;
	private String appMasterTrackingUrl = "";
	
	int containerMem = 1024;
	int containerVCores = 1;
	protected int numTotalContainers = 1;
	private int requestPriority;
	
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	protected AtomicInteger numAllocatedContainers = new AtomicInteger();
	private AtomicInteger numFailedContainers = new AtomicInteger();
	protected AtomicInteger numRequestedContainers = new AtomicInteger();
	
	private List<Thread> launchThreads = new ArrayList<Thread>();
	boolean done;
	
	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler{

		public void onContainersCompleted(List<ContainerStatus> completedContainers) {
			 for (ContainerStatus containerStatus : completedContainers) {    
		          // non complete containers should not be here
		          assert (containerStatus.getState() == ContainerState.COMPLETE);
		          // increment counters for completed/failed containers
		          int exitStatus = containerStatus.getExitStatus();
		          if (0 != exitStatus) {
		            // container failed
		            if (ContainerExitStatus.ABORTED != exitStatus) {
		              // shell script failed
		              // counts as completed
		              numCompletedContainers.incrementAndGet();
		              numFailedContainers.incrementAndGet();
		            } else {
		              // container was killed by framework, possibly preempted
		              // we should re-try as the container was lost for some reason
		              numAllocatedContainers.decrementAndGet();
		              numRequestedContainers.decrementAndGet();
		              // we do not need to release the container as it would be done
		              // by the RM
		            }
		          } else {
		            // nothing to do
		            // container completed successfully
		            numCompletedContainers.incrementAndGet();
		          }
		        }
		      int askCount = numTotalContainers - numRequestedContainers.get();
		      numRequestedContainers.addAndGet(askCount);
		      if (askCount > 0) {
		        for (int i = 0; i < askCount; ++i) {
				    Priority pri = Priority.newInstance(requestPriority);
				    Resource capability = Resource.newInstance(containerMem, containerVCores);
				    ContainerRequest request = new ContainerRequest(capability, null, null, pri);
			        amRMClient.addContainerRequest(request);
		        }
		      }      
		      if (numCompletedContainers.get() == numTotalContainers) {
		        done = true;
		      }
			
		}

		public void onContainersAllocated(List<Container> allocatedContainers) {
		          numAllocatedContainers.addAndGet(allocatedContainers.size());
		          for (Container allocatedContainer : allocatedContainers) {
		            LaunchContainerRunnable runnableLaunchContainer =
		                new LaunchContainerRunnable(allocatedContainer, containerListener);
		            Thread launchThread = new Thread(runnableLaunchContainer);
		            // launch and start the container on a separate thread to keep
		            // the main thread unblocked
		            // as all containers may not be allocated at one go.
		            launchThreads.add(launchThread);
		            launchThread.start();
		          }
			
			
		}

		public void onShutdownRequest() {
			done = true;
			
		}

		public void onNodesUpdated(List<NodeReport> updatedNodes) {
			// TODO Auto-generated method stub
			
		}

		public float getProgress() {
			// TODO Auto-generated method stub
			return 0;
		}

		public void onError(Throwable e) {
			// TODO Auto-generated method stub
			
		}
	 }
	static class NMCallbackHandler implements NMClientAsync.CallbackHandler{

		private ConcurrentHashMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
		private final ApplicationMaster applicationMaster;	
		NMCallbackHandler(ApplicationMaster m){
			applicationMaster = m;
		}
		public void addContainer(ContainerId containerId, Container container) {
		      containers.putIfAbsent(containerId, container);
		}
		public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
			Container container = containers.get(containerId);
		      if (container != null) {
		        applicationMaster.nmClient.getContainerStatusAsync(containerId, container.getNodeId());
		      }	
		}

		public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

		}

		public void onContainerStopped(ContainerId containerId) {
			containers.remove(containerId);
		}

		public void onStartContainerError(ContainerId containerId, Throwable t) {
			  containers.remove(containerId);
		      applicationMaster.numCompletedContainers.incrementAndGet();
		      applicationMaster.numFailedContainers.incrementAndGet();	
		}

		public void onGetContainerStatusError(ContainerId containerId, Throwable t) {			
		}

		public void onStopContainerError(ContainerId containerId, Throwable t) {
			 containers.remove(containerId);
		}
		
	}
	private class LaunchContainerRunnable implements Runnable {

		Container container;
		NMCallbackHandler containerListener;
		
		public LaunchContainerRunnable(
		        Container lcontainer, NMCallbackHandler containerListener) {
		      this.container = lcontainer;
		      this.containerListener = containerListener;
		    }

		public void run() {
			System.out.println("Setting up container launch container for containerid " + container.getId());
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			List<String> commands = new ArrayList<String>();
			Map<String, String> environment = new HashMap<String, String>();
			commands.add(getCommand());
			ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(localResources, environment, commands, null, null, null);
			containerListener.addContainer(container.getId(), container);
			nmClient.startContainerAsync(container, ctx);
		}
		
	}
	String getCommand() {
		StringBuilder sb = new StringBuilder();
		sb.append("echo 'hello world'")
		  .append(";")
		  .append("sleep 1000s");
		return sb.toString();
	}
	ApplicationMaster(){
		conf = new YarnConfiguration();
	}
	
	 private void init() {
		 AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		 amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		 amRMClient.init(conf);
		 amRMClient.start();
		 
		 containerListener = new NMCallbackHandler(this);
		 nmClient = new NMClientAsyncImpl(containerListener);
		 nmClient.init(conf);
		 nmClient.start();
	 }

	 private void run() throws YarnException, IOException {
		 RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, 
					appMasterRpcPort, appMasterTrackingUrl);
		 int maxMem = response.getMaximumResourceCapability().getMemory();
		 int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
		 containerMem = Math.min(maxMem,containerMem);
		 containerVCores = Math.min(maxVCores, containerVCores);
		 
		 List<Container> previousAMRunningContainers = response.getContainersFromPreviousAttempts();
		 numAllocatedContainers.addAndGet(previousAMRunningContainers.size());
		    int numTotalContainersToRequest = numTotalContainers - previousAMRunningContainers.size();
		 for (int i = 0; i < numTotalContainersToRequest; ++i) {
			    Priority pri = Priority.newInstance(requestPriority);
			    Resource capability = Resource.newInstance(containerMem, containerVCores);
			    ContainerRequest request = new ContainerRequest(capability, null, null, pri);
		        amRMClient.addContainerRequest(request);
		      }
		  numRequestedContainers.set(numTotalContainers);    
	 }
	 
	 private boolean finish() throws YarnException, IOException, InterruptedException {
		 while(!done && (numCompletedContainers.get() != numTotalContainers)) {
			Thread.sleep(200);
		}
		 nmClient.stop();
		
		FinalApplicationStatus appStatus;
        String appMessage = "print hello world";
        boolean success = true;
		if(numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
			appStatus = FinalApplicationStatus.SUCCEEDED;
		}else {
			appStatus = FinalApplicationStatus.FAILED;
		}
        amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        amRMClient.stop();
        return true;
	}
	
	public static void main(String[] args) throws IOException, YarnException, InterruptedException {
			boolean result = false;
			ApplicationMaster appMaster = new ApplicationMaster();
			appMaster.init();
			appMaster.run();
			result = appMaster.finish();
			if(result) {
				System.exit(0);
			}else {
				System.exit(2);
			}
    }
}

 
	