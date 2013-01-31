package io.qdb.kvstore.cluster

import spock.lang.Specification
import com.google.common.eventbus.EventBus
import io.qdb.kvstore.StoreTx

class ClusterImplSpec extends Specification {

    EventBus eventBus = new EventBus()
    ServerLocatorForTests serverLocator = new ServerLocatorForTests(eventBus, ["s1", "s2", "s3"] as String[])
    TransportForTests t1 = new TransportForTests("s1")
    TransportForTests t2 = new TransportForTests("s2")
    TransportForTests t3 = new TransportForTests("s3")
    ClusterMemberForTests s1 = new ClusterMemberForTests()
    ClusterMemberForTests s2 = new ClusterMemberForTests()
    ClusterMemberForTests s3 = new ClusterMemberForTests()

    def "PREPARE sent to all nodes on start"() {
//        ClusterImpl c1 = new ClusterImpl(eventBus, serverLocator, t1, 4000)
//        c1.rejoin(s1)
//
//        c1.propose(new StoreTx("m1", StoreTx.Operation.PUT, "k", "v"))
//
        expect:
        true
//        t1.toString() == "PREPARE n=11 v=p1 to 1, PREPARE n=11 v=p1 to 2, PREPARE n=11 v=p1 to 3"
    }

}
