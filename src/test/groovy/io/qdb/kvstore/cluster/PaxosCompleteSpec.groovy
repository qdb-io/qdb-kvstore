package io.qdb.kvstore.cluster

import spock.lang.Stepwise

@Stepwise
class PaxosCompleteSpec extends PaxosNonSharedBase {

    def "Single proposal accepted"() {
        s1.propose("p1")
        transport.deliver("PREPARE")
        transport.deliver("PROMISE")
        transport.deliver("ACCEPT")
        transport.deliver("ACCEPTED")

        expect:
        listener1.ourProposalAccepted == "p1"
        listener1.proposalAccepted == null
        listener2.ourProposalAccepted == null
        listener2.proposalAccepted == "p1"
        listener3.ourProposalAccepted == null
        listener3.proposalAccepted == "p1"
    }

}