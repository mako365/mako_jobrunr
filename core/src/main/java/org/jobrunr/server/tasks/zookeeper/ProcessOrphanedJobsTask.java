package org.jobrunr.server.tasks.zookeeper;

import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.states.JobState;
import org.jobrunr.jobs.states.ProcessingState;
import org.jobrunr.server.BackgroundJobServer;
import org.jobrunr.storage.BackgroundJobServerStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static org.jobrunr.jobs.states.StateName.PROCESSING;
import static org.jobrunr.storage.Paging.AmountBasedList.ascOnUpdatedAt;

public class ProcessOrphanedJobsTask extends AbstractJobZooKeeperTask {

    private final int pageRequestSize;
    private final Duration serverTimeoutDuration;

    public ProcessOrphanedJobsTask(BackgroundJobServer backgroundJobServer) {
        super(backgroundJobServer);
        this.pageRequestSize = backgroundJobServer.getConfiguration().getOrphanedJobsRequestSize();
        this.serverTimeoutDuration = backgroundJobServer.getConfiguration().getPollInterval().multipliedBy(backgroundJobServer.getConfiguration().getServerTimeoutPollIntervalMultiplicand());
    }

    @Override
    protected void runTask() {
        LOGGER.trace("Looking for orphan jobs... ");
        final Instant updatedBefore = runStartTime().minus(serverTimeoutDuration);
        processManyJobs(previousResults -> getOrphanedJobs(updatedBefore, previousResults),
                this::changeJobStateToFailedAndRunJobFilter,
                totalAmountOfOrphanedJobs -> LOGGER.debug("Found {} orphan jobs.", totalAmountOfOrphanedJobs));
    }

    private List<Job> getOrphanedJobs(Instant updatedBefore, List<Job> previousResults) {
        if (previousResults != null && previousResults.size() < pageRequestSize) return emptyList();
        return storageProvider.getJobList(PROCESSING, updatedBefore, ascOnUpdatedAt(pageRequestSize));
    }

    private void changeJobStateToFailedAndRunJobFilter(Job job) {
        JobState jobState = job.getJobState();
        if (jobState != null && PROCESSING.equals(jobState.getName())) {
            ProcessingState processingState = (ProcessingState) jobState;
            UUID serverId = processingState.getServerId();
            List<BackgroundJobServerStatus> jobServers = storageProvider.getBackgroundJobServers();
            if (jobServers.stream().anyMatch(jobServer -> jobServer.getId().equals(serverId))) {
                LOGGER.warn("Found orphan job {} but keep it running since the server is still alive.", job.getId());
                return;
            }
        }
        IllegalThreadStateException e = new IllegalThreadStateException("Job was too long in PROCESSING state without being updated.");
        jobFilterUtils.runOnJobProcessingFailedFilters(job, e);
        job.failed("Orphaned job", e);
    }
}
