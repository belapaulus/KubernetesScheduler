package cws.k8s.scheduler.dag;

import lombok.*;

@Getter
@ToString
@RequiredArgsConstructor
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
public class Edge {

    final int uid;
    final String label = null;
    final long from;
    final long to;

}
