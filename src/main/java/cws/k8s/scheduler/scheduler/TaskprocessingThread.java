package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.Task;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class TaskprocessingThread extends Thread {

    private final List<Task> unprocessedTasks;
    private final Function<List<Task>, Integer> function;

    @Override
    public void run() {
        int unscheduled = 0;
        while(!Thread.interrupted()){
            try{
                LinkedList<Task> tasks;
                synchronized (unprocessedTasks) {
                    do {
                        if (unscheduled == unprocessedTasks.size()) {
                            unprocessedTasks.wait( 10000 );
                        }
                        if( Thread.interrupted() ) {
                            return;
                        }
                        // so lange unprocessedTasks leer ist kommen wir hier nicht raus.
                        // wir geben das lock aber nur auf, wenn auch unser integer unscheduled == 0
                        // sonst machen wir busy wait
                        // wahrscheinlich kann der fall, dass unprocessedTasks leer ist, aber unscheduled != 0
                        // nicht eintreten
                        // dementsprechend ist die if bedingung hier eher ein zusatzliches wait für den fall,
                        // dass unprocessedTasks nicht leer ist, wir aber wissen (da wir es gerade probiert haben)
                        // dass sich die restlichen Tasks nicht schedulen lassen, also warten wir auf ein signal
                        // dass sich etwas geändert hat.
                    } while ( unprocessedTasks.isEmpty() );
                    // wir kopieren die liste der unprocessed tasks
                    tasks = new LinkedList<>(unprocessedTasks);
                }
                // und rufen unsere funktion (schedule oder finish)
                // anders gesagt wir rufen die funktion immer wieder wenn unprocessed tasks nicht leer ist
                // außer die länge von unprocessed tasks entspricht der anzahl an tasks die wir im letzten
                // versuch nicht schedulen konnten. in dem fall warten wir.
                unscheduled = function.apply( tasks );
            } catch (InterruptedException e){
                Thread.currentThread().interrupt();
            } catch (Exception e){
                unscheduled = -1;
                log.info("Error while processing",e);
            }
        }
    }
}
