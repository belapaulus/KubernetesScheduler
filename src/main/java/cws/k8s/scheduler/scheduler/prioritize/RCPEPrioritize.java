package cws.k8s.scheduler.scheduler.prioritize;

import cws.k8s.scheduler.model.Task;

import java.util.List;

public class RCPEPrioritize implements Prioritize {

    @Override
    public void sortTasks( List<Task> tasks ) {
        tasks.sort( ( o1, o2 ) -> {
            int i1, i2;
            i1 = o1.getVertex().getRcpeRank();
            i2 = o2.getVertex().getRcpeRank();
            if (i1 != i2) {
                return Integer.signum(i2 - i1);
            }
            i1 = o1.getVertex().getRcpe().size();
            i2 = o2.getVertex().getRcpe().size();
            if (i1 != i2) {
                return Integer.signum(i2 - i1);
            }
            return Integer.signum( o2.getId() - o1.getId() );
        } );
    }

}
