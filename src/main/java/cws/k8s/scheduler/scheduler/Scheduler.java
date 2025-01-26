package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.model.*;
import cws.k8s.scheduler.util.Batch;
import cws.k8s.scheduler.client.Informable;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

@Slf4j
public abstract class Scheduler implements Informable {

    //Visible variables
    @Getter
    private final String name;
    @Getter
    private final String execution;
    @Getter
    private final String namespace;
    @Getter
    private final String dns;
    @Getter
    private boolean close;
    @Getter
    private final DAG dag;

    private final Object batchHelper = new Object();
    private int currentBatch = 0;
    private Batch currentBatchInstance = null;

    final KubernetesClient client;
    private final Set<Task> upcomingTasks = new HashSet<>();
    private final List<Task> unscheduledTasks = new ArrayList<>( 100 );
    private final List<Task> unfinishedTasks = new ArrayList<>( 100 );
    final Map<String, Task> tasksByPodName = new HashMap<>();
    final Map<Long, Task> tasksById = new HashMap<>();
    private final Watch watcher;
    private final TaskprocessingThread schedulingThread;
    private final TaskprocessingThread finishThread;

    final boolean traceEnabled;

    Scheduler(String execution, KubernetesClient client, String namespace, SchedulerConfig config){
        this.execution = execution;
        this.name = System.getenv( "SCHEDULER_NAME" ) + "-" + execution;
        this.namespace = namespace;
        log.trace("Register scheduler for {}", this.name);
        this.client = client;
        this.dns = config.dns.endsWith( "/" ) ? config.dns : config.dns + "/";
        this.dag = new DAG();
        this.traceEnabled = config.traceEnabled;

        PodWatcher podWatcher = new PodWatcher(this);

        schedulingThread = new TaskprocessingThread( unscheduledTasks, this::schedule );
        schedulingThread.start();

        finishThread = new TaskprocessingThread(unfinishedTasks, this::terminateTasks );
        finishThread.start();

        watcher = client.pods().inNamespace( this.namespace ).watch(podWatcher);
        log.info("registered pod watcher for execution {}", this.execution);
    }

    /* Abstract methods */

    /**
     * @return the number of unscheduled Tasks
     */
    public int schedule( final List<Task> unscheduledTasks ) {
        long startSchedule = System.currentTimeMillis();
        if( traceEnabled ) {
            unscheduledTasks.forEach( x -> x.getTraceRecord().tryToSchedule( startSchedule ) );
        }
        final ScheduleObject scheduleObject = getTaskNodeAlignment(unscheduledTasks, getAvailableByNode());
        final List<NodeTaskAlignment> taskNodeAlignment = scheduleObject.getTaskAlignments();

        //check if still possible...
        if ( scheduleObject.isCheckStillPossible() ) {
            boolean possible = validSchedulePlan ( taskNodeAlignment );
            if (!possible) {
                log.info("The whole scheduling plan is not possible anymore.");
                return taskNodeAlignment.size();
            }
        }

        int failure = 0;
        int scheduled = 0;
        for (NodeTaskAlignment nodeTaskAlignment : taskNodeAlignment) {
            try {
                if (isClose()) {
                    return -1;
                }
                if ( !assignTaskToNode( nodeTaskAlignment ) ){
                    if ( scheduleObject.isStopSubmitIfOneFails() ) {
                        return taskNodeAlignment.size() - scheduled;
                    }
                    failure++;
                    continue;
                }
            } catch ( Exception e ){
                log.info( "Could not schedule task: {} undo all", nodeTaskAlignment.task.getConfig().getRunName() );
                e.printStackTrace();
                undoTaskScheduling( nodeTaskAlignment.task );
                if ( scheduleObject.isStopSubmitIfOneFails() ) {
                    return taskNodeAlignment.size() - scheduled;
                }
                continue;
            }
            taskWasScheduled(nodeTaskAlignment.task);
            scheduled++;
        }
        return unscheduledTasks.size() - taskNodeAlignment.size() + failure;
    }

    /**
     * Call this method in case of any scheduling problems
     */
    void undoTaskScheduling( Task task ){}


    public boolean validSchedulePlan( List<NodeTaskAlignment> taskNodeAlignment ){
        Map< NodeWithAlloc, Requirements> availableByNode = getAvailableByNode();
        for ( NodeTaskAlignment nodeTaskAlignment : taskNodeAlignment ) {
            final Requirements requirements = availableByNode.get(nodeTaskAlignment.node);
            if ( requirements == null ) {
                return false;
            }
            requirements.subFromThis(nodeTaskAlignment.task.getPod().getRequest());
        }
        for ( Map.Entry<NodeWithAlloc, Requirements> e : availableByNode.entrySet() ) {
            if ( ! e.getValue().higherOrEquals( Requirements.ZERO ) ) {
                return false;
            }
        }
        return true;
    }

    abstract ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
    );

    int terminateTasks( final List<Task> finishedTasks ) {
        for (Task finishedTask : finishedTasks) {
            taskWasFinished( finishedTask );
        }
        return 0;
    }

    /* Pod Event */

    void podEventReceived(Watcher.Action action, Pod pod){}

    void onPodTermination( PodWithAge pod ){
        Task t = changeStateOfTask( pod, State.PROCESSING_FINISHED );

        //If null, task was already changed
        if( t == null ) {
            return;
        }
        t.setPod( pod );

        synchronized (unfinishedTasks){
            unfinishedTasks.add( t );
            unfinishedTasks.notifyAll();
        }
    }

    void taskWasFinished( Task task ){
        synchronized (unfinishedTasks){
            unfinishedTasks.remove( task );
        }
        task.getState().setState(task.wasSuccessfullyExecuted() ? State.FINISHED : State.FINISHED_WITH_ERROR);
    }

    public void schedulePod(PodWithAge pod ) {
        log.info("schedule pod called for pod {}", pod.getName());
        Task task = changeStateOfTask( pod, State.UNSCHEDULED );
        //If null, task was already unscheduled
        if ( task == null ) {
            return;
        }
        task.setPod( pod );
        if ( task.getBatch() == null ){
            synchronized (unscheduledTasks){
                unscheduledTasks.add( task );
                unscheduledTasks.notifyAll();
                synchronized ( upcomingTasks ){
                    upcomingTasks.remove( task );
                }
            }
        } else {
            Batch batch = task.getBatch();
            log.info("pod belongs to batch {}", batch.id);
            batch.informSchedulable( task );
            log.info("moved task from unready to ready. task.name {}, task.id {}", task.getConfig().getRunName(), task.getId());
            log.info("trying to schedule batch");
            synchronized (batchHelper) {
                tryToScheduleBatch( batch );
            }
        }
    }

    /**
     * Synchronize calls via batchHelper
     */
    private void tryToScheduleBatch( Batch batch ){
        if ( batch.canSchedule() ){
            synchronized (unscheduledTasks){
                final List<Task> tasksToScheduleAndDestroy = batch.getTasksToScheduleAndDestroy();
                unscheduledTasks.addAll(tasksToScheduleAndDestroy);
                synchronized ( upcomingTasks ){
                    tasksToScheduleAndDestroy.forEach(upcomingTasks::remove);
                }
                log.info("added tasks from batch to unscheduledTasks. tasks in batch: {}, unscheduled tasks: {}", tasksToScheduleAndDestroy, unscheduledTasks);
                log.info("notifying task processing threads");
                unscheduledTasks.notifyAll();
            }
        } else {
            log.info("could not yet schedule batch because batch.closed is {} and len(batch.unready) is {}", batch.isClosed(), batch.lenUnready());
        }
    }

    void taskWasScheduled(Task task ) {
        synchronized (unscheduledTasks){
            unscheduledTasks.remove( task );
        }
        taskWasScheduledSetState( task );
    }

    void taskWasScheduledSetState( Task task ){
        task.getState().setState( State.PREPARED );
    }

    public void markPodAsDeleted( PodWithAge pod ) {
        final Task task = changeStateOfTask(pod, State.DELETED);
        task.setPod( pod );
    }

    /* External access to Tasks */

    public void addTask( long id, TaskConfig conf ) {
        synchronized (tasksById) {
            if (tasksById.containsKey(id)) {
                log.info("ignoring task with known id: {}", id);
                return;
            }
        }
        final Task task = new Task( conf, dag );
        synchronized ( tasksByPodName ) {
            if (tasksByPodName.containsKey( conf.getRunName() )) {
                throw new RuntimeException("trying to add task with runname that is already in the list");
            }
            tasksByPodName.put( conf.getRunName(), task );
        }
        synchronized ( tasksById ) {
            if (tasksById.containsKey( id )) {
                throw new RuntimeException("trying to add task with id that is already in the list");
            }
            tasksById.put( id, task );
        }
        synchronized ( upcomingTasks ) {
            upcomingTasks.add( task );
        }
        log.info("added task to tasksByPodName, tasksById and upcomingTasks. TaskId: {}, PodName: {}", id, conf.getRunName());
        if( currentBatchInstance != null ){
            currentBatchInstance.registerTask( task );
            log.info("added task to unready list in batch");
        } else {
            log.info("task not added to batch as current batch instance is null");
        }
    }

    public boolean removeTask( long id ) {
        final Task task;
        synchronized ( tasksById ) {
            task = tasksById.get( id );
            }
        if ( task == null ) {
            return false;
        }
        synchronized ( tasksByPodName ) {
            tasksByPodName.remove( task.getConfig().getRunName() );
        }
        synchronized ( upcomingTasks ) {
            upcomingTasks.remove( task );
        }
        return true;
    }

    /**
     * Chooses best param for a task
     */
    public Map<String, Object> getSchedulerParams( String taskname, String name ){
        return new HashMap<>();
    }

    public TaskState getTaskState( long id ) {
        synchronized ( tasksById ) {
            if ( tasksById.containsKey( id ) ) {
                return tasksById.get( id ).getState();
            }
        }
        return null;
    }

    /* Nodes */

    /**
     * Checks if a node fulfills all requirements for a pod. This means: <br>
     * - enough resources available <br>
     * - Affinities match
     */
    public boolean canSchedulePodOnNode(Requirements availableByNode, PodWithAge pod, NodeWithAlloc node ) {
        if ( availableByNode == null ) {
            return false;
        }
        return node.canScheduleNewPod()
                && availableByNode.higherOrEquals( pod.getRequest() )
                && affinitiesMatch( pod, node );
    }

    boolean affinitiesMatch( PodWithAge pod, NodeWithAlloc node ){
        final Map<String, String> podsNodeSelector = pod.getSpec().getNodeSelector();
        final Map<String, String> nodesLabels = node.getMetadata().getLabels();
        if ( podsNodeSelector == null || podsNodeSelector.isEmpty() ) {
            return true;
        }
        //cannot be fulfilled if podsNodeSelector is not empty
        if ( nodesLabels == null || nodesLabels.isEmpty() ) {
            return false;
        }

        return nodesLabels.entrySet().containsAll( podsNodeSelector.entrySet() );
    }

    public void newNode(NodeWithAlloc node) {
        informResourceChange();
    }

    public void removedNode(NodeWithAlloc node) {}

    List<NodeWithAlloc> getNodeList(){
        return client.getAllNodes();
    }

    /**
     * You may extend this method
     */
    boolean canPodBeScheduled( PodWithAge pod, NodeWithAlloc node ){
        return node.canSchedule( pod );
    }

    boolean assignTaskToNode( NodeTaskAlignment alignment ){

        final File nodeFile = new File(alignment.task.getWorkingDir() + '/' + ".command.node");

        try(BufferedWriter printWriter = new BufferedWriter( new FileWriter( nodeFile ))){
            printWriter.write( alignment.node.getName() );
            printWriter.write( '\n' );
        } catch (IOException e) {
            log.error( "Cannot read " + nodeFile, e);
        }

        alignment.task.setNode( alignment.node );

        final PodWithAge pod = alignment.task.getPod();

        alignment.node.addPod( pod, alignment.task.isCopiesDataToNode() );

        log.info ( "Assign pod: " + pod.getMetadata().getName() + " to node: " + alignment.node.getMetadata().getName() );

        client.assignPodToNode( pod, alignment.node.getMetadata().getName() );

        pod.getSpec().setNodeName( alignment.node.getMetadata().getName() );
        log.info ( "Assigned pod to:" + pod.getSpec().getNodeName());

        alignment.task.submitted();
        if( traceEnabled ) {
            alignment.task.getTraceRecord().submitted();
            alignment.task.writeTrace();
        }

        return true;
    }

    /* Helper */

    public void startBatch(){
        // batchHelper ist ein leeres Objekt, dass scheinbar nur zur synchronisation dient
        synchronized (batchHelper){
            if ( currentBatchInstance == null || currentBatchInstance.isClosed() ){
                currentBatchInstance = new Batch( currentBatch++ );
                log.info("set current batch instance to batch with id {}", currentBatchInstance.id);
            } else {
                log.info("ignored start batch request because there is an open batch");
            }
        }
    }

    public void endBatch( int tasksInBatch ){
        synchronized (batchHelper){
            currentBatchInstance.close( tasksInBatch );
            log.info("closed current batch instance");
            log.info("trying to schedul batch");
            tryToScheduleBatch( currentBatchInstance );
        }
    }

    /**
     * @return returns the task, if the state was changed
     */
    Task changeStateOfTask(Pod pod, State state) {
        Task t = getTaskByPod(pod);
        if (t != null) {
            synchronized (t.getState()) {
                if (t.getState().getState().level < state.level) {
                    t.getState().setState(state);
                    return t;
                } else {
                    log.debug("Task {} was already in state {} and cannot be changed to {}", t.getConfig().getRunName(),
                            t.getState().getState(), state);
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * starts the scheduling routine
     */
    public void informResourceChange() {
        synchronized (unscheduledTasks){
            unscheduledTasks.notifyAll();
        }
    }

    Task getTaskByPod( Pod pod ) {
        Task t = null;
        synchronized ( tasksByPodName ) {
            if ( tasksByPodName.containsKey( pod.getMetadata().getName() ) ) {
                t = tasksByPodName.get( pod.getMetadata().getName() );
            }
        }

        if ( t == null ){
            throw new IllegalStateException( "No task with config found for: " + pod.getMetadata().getName() );
        }

        return t;
    }

    Map<NodeWithAlloc, Requirements> getAvailableByNode(){
        Map<NodeWithAlloc, Requirements> availableByNode = new HashMap<>();
        List<String> logInfo = new LinkedList<>();
        logInfo.add("------------------------------------");
        for (NodeWithAlloc item : getNodeList()) {
            if ( !item.isReady() ) {
                continue;
            }
            final Requirements availableResources = item.getAvailableResources();
            availableByNode.put(item, availableResources);
            logInfo.add("Node: " + item.getName() + " " + availableResources);
        }
        logInfo.add("------------------------------------");
        log.info(String.join("\n", logInfo));
        return availableByNode;
    }

    /**
     * Filters all nodes, that have enough resources and fulfill the affinities
     */
    public Set<NodeWithAlloc> getMatchingNodesForTask( Map<NodeWithAlloc, Requirements> availableByNode, Task task ){
        Set<NodeWithAlloc> result = new HashSet<>();
        for (Map.Entry<NodeWithAlloc, Requirements> entry : availableByNode.entrySet()) {
            if ( this.canSchedulePodOnNode( entry.getValue(), task.getPod(), entry.getKey() ) ){
                result.add( entry.getKey() );
            }
        }
        return result;
    }

    LinkedList<Task> getUpcomingTasksCopy() {
        return new LinkedList<>( upcomingTasks );
    }

    /**
     * Close used resources
     */
    public void close(){
        watcher.close();
        schedulingThread.interrupt();
        finishThread.interrupt();
        this.close = true;
    }

    static class PodWatcher implements Watcher<Pod> {

        private final Scheduler scheduler;

        public PodWatcher(Scheduler scheduler) {
            this.scheduler = scheduler;
        }

        @Override
        public void eventReceived(Action action, Pod pod) {

            scheduler.podEventReceived(action, pod);

            if (!scheduler.name.equals(pod.getSpec().getSchedulerName())) {
                return;
            }

            PodWithAge pwa = new PodWithAge(pod);
            if (pod.getMetadata().getLabels() != null) {
                log.debug("Got pod: " + pod.getMetadata().getName() +
                        " app: " + pod.getMetadata().getLabels().getOrDefault("app", "-") +
                        " processName: " + pod.getMetadata().getLabels().getOrDefault("processName", "-") +
                        " runName: " + pod.getMetadata().getLabels().getOrDefault("runName", "-") +
                        " taskName: " + pod.getMetadata().getLabels().getOrDefault("taskName", "-") +
                        " scheduler: " + pwa.getSpec().getSchedulerName() +
                        " action: " + action
                );
            } else {
                log.debug("Got pod " + pod.getMetadata().getName() + " scheduler: " + pwa.getSpec().getSchedulerName());
            }


            switch (action) {
                case ADDED:
                    if ( pwa.getSpec().getNodeName() == null ) {
                        scheduler.schedulePod( pwa );
                    }
                    break;
                case MODIFIED:
                    if (!pod.getStatus().getContainerStatuses().isEmpty() && pod.getStatus().getContainerStatuses().get(0).getState().getTerminated() != null) {
                        scheduler.onPodTermination(pwa);
                    } else {
                        final Task task = scheduler.getTaskByPod(pwa);
                        task.setPod( pwa );
                    }
                    break;
                case DELETED:
                    scheduler.markPodAsDeleted(pwa);
                    break;
                default: log.info( "No implementation for {}", action );
            }

        }


        @Override
        public void onClose(WatcherException cause) {
            log.info( "Watcher was closed" );
        }

    }


}
