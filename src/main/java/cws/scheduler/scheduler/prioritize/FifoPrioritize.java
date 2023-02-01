package fonda.scheduler.scheduler.prioritize;

import fonda.scheduler.model.Task;

import java.util.Comparator;
import java.util.List;

public class FifoPrioritize implements Prioritize {

    @Override
    public void sortTasks( List<Task> tasks ) {
        tasks.sort( Comparator.comparing( Task::getId ) );
    }

}
