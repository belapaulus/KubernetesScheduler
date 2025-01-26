package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;

import java.util.List;

public class RankMaxPrioritize implements Prioritize {

    @Override
    public void sortTasks( List<Task> tasks ) {
        tasks.sort( ( o1, o2 ) -> {
            if ( o1.getVertex().getUpwardRank() == o2.getVertex().getUpwardRank() ) {
                return Long.signum( o2.getInputSize() - o1.getInputSize() );
            }
            //Prefer larger ranks
            return Integer.signum( o2.getVertex().getUpwardRank() - o1.getVertex().getUpwardRank() );
        } );
    }

}
