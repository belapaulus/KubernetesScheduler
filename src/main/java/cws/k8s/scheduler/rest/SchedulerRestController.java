package cws.k8s.scheduler.rest;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.Edge;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.PrioritizeAssignScheduler;
import cws.k8s.scheduler.scheduler.Scheduler;
import cws.k8s.scheduler.scheduler.prioritize.*;
import cws.k8s.scheduler.scheduler.nodeassign.FairAssign;
import cws.k8s.scheduler.scheduler.nodeassign.NodeAssign;
import cws.k8s.scheduler.scheduler.nodeassign.RandomNodeAssign;
import cws.k8s.scheduler.scheduler.nodeassign.RoundRobinAssign;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
@EnableScheduling
public class SchedulerRestController {

    private final KubernetesClient client;
    private final boolean autoClose;
    private final ApplicationContext appContext;
    private long closedLastScheduler = -1;

    /**
     * Holds the scheduler for one execution
     * Execution: String in lowercase
     * Scheduler: An instance of a scheduler with the requested type
     */
    private static final Map<String, Scheduler> schedulerHolder = new HashMap<>();

    public SchedulerRestController(
            @Autowired KubernetesClient client,
            @Value("#{environment.AUTOCLOSE}") String autoClose,
            @Autowired ApplicationContext appContext ){
        this.client = client;
        this.autoClose = Boolean.parseBoolean(autoClose);
        this.appContext = appContext;
    }

    @Scheduled(fixedDelay = 5000)
    public void close() throws InterruptedException {
        if ( autoClose && schedulerHolder.isEmpty() && closedLastScheduler != -1 ) {
            Thread.sleep( System.currentTimeMillis() - closedLastScheduler + 5000 );
            if ( schedulerHolder.isEmpty() ) {
                SpringApplication.exit(appContext, () -> 0);
            }
        }
    }

    private ResponseEntity<String> noSchedulerFor( String execution ){
        log.warn( "No scheduler for execution: {}", execution );
        return new ResponseEntity<>( "There is no scheduler for " + execution, HttpStatus.BAD_REQUEST );
    }

    @Operation(summary = "Register a new execution")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Execution successfully registered",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Scheduling algorithm or cost function not found",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "Scheduling algorithm does not work with current config",
                    content = @Content) })
    /**
     * Register a scheduler for a workflow execution
     * @param execution unique name of the execution
     * @param config Additional parameters for the scheduler
     * @return
     */
    @PostMapping("/v1/scheduler/{execution}")
    ResponseEntity<String> registerScheduler(
            @PathVariable String execution,
            @RequestBody(required = false) SchedulerConfig config
    ) {

        final String namespace = config.namespace;
        final String strategy = config.strategy;
        log.info( "receive register execution request. execution is {} with config {}", execution, config );

        Scheduler scheduler;


        if ( schedulerHolder.containsKey( execution ) ) {
            return noSchedulerFor( execution );
        }

        switch ( strategy.toLowerCase() ){
            default: {
                final String[] split = strategy.split( "-" );
                Prioritize prioritize;
                NodeAssign assign;
                if ( split.length <= 2 ) {
                    switch ( split[0].toLowerCase() ) {
                        case "fifo": prioritize = new FifoPrioritize(); break;
                        case "rcpe": prioritize = new RCPEPrioritize(); break;
                        case "rank": prioritize = new RankPrioritize(); break;
                        case "rankmin": prioritize = new RankMinPrioritize(); break;
                        case "rankmax": prioritize = new RankMaxPrioritize(); break;
                        case "random": case "r": prioritize = new RandomPrioritize(); break;
                        case "max": prioritize = new MaxInputPrioritize(); break;
                        case "min": prioritize = new MinInputPrioritize(); break;
                        default:
                            return new ResponseEntity<>( "No Prioritize for: " + split[0], HttpStatus.NOT_FOUND );
                    }
                    if ( split.length == 2 ) {
                        switch ( split[1].toLowerCase() ) {
                            case "random": case "r": assign = new RandomNodeAssign(); break;
                            case "roundrobin": case "rr": assign = new RoundRobinAssign(); break;
                            case "fair": case "f": assign = new FairAssign(); break;
                            default:
                                return new ResponseEntity<>( "No Assign for: " + split[1], HttpStatus.NOT_FOUND );
                        }
                    } else {
                        assign = new RoundRobinAssign();
                    }
                    scheduler = new PrioritizeAssignScheduler( execution, client, namespace, config, prioritize, assign );
                } else {
                    return new ResponseEntity<>( "No scheduler for strategy: " + strategy, HttpStatus.NOT_FOUND );
                }
            }
        }

        schedulerHolder.put( execution, scheduler );
        client.addInformable( scheduler );
        log.info("registered scheduler as informable with kubernetes client");

        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "Register a task for execution")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Task successfully registered",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    /**
     * Register a task for the execution
     *
     * @param execution unique name of the execution
     * @param config The config contains the task name, input files, and optional task parameter the scheduler has to determine
     * @return Parameters the scheduler suggests for the task
     */
    @PostMapping("/v1/scheduler/{execution}/task/{id}")
    ResponseEntity<?> registerTask(@PathVariable String execution, @PathVariable long id, @RequestBody TaskConfig config ) {

        log.trace( execution + " " + config.getTask() + " got: " + config );
        log.info("received register task request for execution {}. task name is {}", execution, config.getTask());

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        scheduler.addTask( id, config );
        Map<String, Object> schedulerParams = scheduler.getSchedulerParams( config.getTask(), config.getName() );

        return new ResponseEntity<>( schedulerParams, HttpStatus.OK );

    }


    @Operation(summary = "Delete a task of execution")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Task successfully deleted",
                    content = @Content),
            @ApiResponse(responseCode = "404", description = "Task not found",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    /**
     * Delete a task, only works if the batch of the task was closed and if no pod was yet submitted.
     * If a pod was submitted, delete the pod instead.
     *
     * @param execution
     * @param id
     * @return
     */
    @DeleteMapping("/v1/scheduler/{execution}/task/{id}")
    ResponseEntity<? extends Object> deleteTask( @PathVariable String execution, @PathVariable long id ) {

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        final boolean found = scheduler.removeTask( id );
        return new ResponseEntity<>( found ? HttpStatus.OK : HttpStatus.NOT_FOUND );

    }

    @Operation(summary = "")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully started batch",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @PutMapping("/v1/scheduler/{execution}/startBatch")
    ResponseEntity<String> startBatch( @PathVariable String execution ) {

        log.info("received start batch request for execution {}", execution);
        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }
        scheduler.startBatch();
        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "End a batch")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully ended batch",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @PutMapping("/v1/scheduler/{execution}/endBatch")
    ResponseEntity<String> endBatch( @PathVariable String execution, @RequestBody int tasksInBatch ) {

        log.info("received end batch request for execution {}", execution);
        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }
        scheduler.endBatch( tasksInBatch );
        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "Check the state of a task")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Task found",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    /**
     * Check Task state
     *
     * @param execution unique name of the execution
     * @param id        unique id of task
     * @return boolean
     */
    @GetMapping("/v1/scheduler/{execution}/task/{id}")
    ResponseEntity<?> getTaskState(@PathVariable String execution, @PathVariable long id ) {

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        return new ResponseEntity<>( scheduler.getTaskState( id ), HttpStatus.OK );

    }

    @Operation(summary = "Delete an execution after it has finished or crashed")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Execution successfully deleted",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    /**
     * Call this after the execution has finished
     *
     * @param execution unique name of the execution
     * @return
     */
    @DeleteMapping("/v1/scheduler/{execution}")
    ResponseEntity<String> delete( @PathVariable String execution ) {

        log.info( "Delete scheduler: " + execution );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }
        schedulerHolder.remove( execution );
        client.removeInformable( scheduler );
        scheduler.close();
        closedLastScheduler = System.currentTimeMillis();
        return new ResponseEntity<>( HttpStatus.OK );
    }

    @GetMapping ("/health")
    ResponseEntity<Object> checkHealth() {
        return new ResponseEntity<>( HttpStatus.OK );
    }

    @Operation(summary = "Register DAG vertices")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Vertices successfully registered",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @PostMapping("/v1/scheduler/{execution}/DAG/vertices")
    ResponseEntity<String> addVertices( @PathVariable String execution, @RequestBody List<Vertex> vertices ) {

        log.trace( "submit vertices: {}", vertices );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }
        log.info("dag is empty: {}", scheduler.getDag().isEmpty());
        scheduler.getDag().registerVertices( vertices);

        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "Delete DAG vertices")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Vertices successfully removed",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @DeleteMapping("/v1/scheduler/{execution}/DAG/vertices")
    ResponseEntity<String> deleteVertices( @PathVariable String execution, @RequestBody long[] vertices ) {

        log.trace( "submit vertices: {}", vertices );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        scheduler.getDag().removeVertices( vertices );

        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "Register DAG edges")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Edges successfully registered",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @PostMapping("/v1/scheduler/{execution}/DAG/edges")
    ResponseEntity<String> addEdges( @PathVariable String execution, @RequestBody List<Edge> edges ) {

        log.trace( "submit edges: {}", edges );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        final DAG dag = scheduler.getDag();
        dag.registerEdges( edges );

        return new ResponseEntity<>( HttpStatus.OK );

    }

    @Operation(summary = "Delete DAG edges")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Edges successfully removed",
                    content = @Content),
            @ApiResponse(responseCode = "400", description = "No scheduler found for this execution",
                    content = @Content) })
    @DeleteMapping("/v1/scheduler/{execution}/DAG/edges")
    ResponseEntity<String> deleteEdges( @PathVariable String execution, @RequestBody int[] edges ) {

        log.trace( "submit edges: {}", edges );

        final Scheduler scheduler = schedulerHolder.get( execution );
        if ( scheduler == null ) {
            return noSchedulerFor( execution );
        }

        final DAG dag = scheduler.getDag();
        dag.removeEdges( edges );

        return new ResponseEntity<>( HttpStatus.OK );

    }

}
