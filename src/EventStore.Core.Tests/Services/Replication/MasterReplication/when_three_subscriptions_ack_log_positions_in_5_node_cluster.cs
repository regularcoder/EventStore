using System;
using System.Net;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Replication.MasterReplication
{
    [TestFixture]
    [Category("UnderTest")]
    public class when_three_subscriptions_ack_log_positions_in_5_node_cluster : with_master_replication_service
    {
        private long _ackLogPosition1;
        private long _ackLogPosition2;
        private long _ackLogPosition3;

        private Guid _subscriptionId1 = Guid.NewGuid();
        private Guid _subscriptionId2 = Guid.NewGuid();
        private Guid _subscriptionId3 = Guid.NewGuid();
        private Guid _epochId = Guid.NewGuid();

        public when_three_subscriptions_ack_log_positions_in_5_node_cluster()
        {
            ClusterSize = 5;
        }

        public override void WriteTestScenario()
        {
            WriteSingleEvent("test-stream", 0, new String('*', 50));
            _ackLogPosition1 = Db.Config.WriterCheckpoint.ReadNonFlushed();

            WriteSingleEvent("test-stream", 1, new String('*', 50));
            _ackLogPosition2 = Db.Config.WriterCheckpoint.ReadNonFlushed();

            WriteSingleEvent("test-stream", 2, new String('*', 50));
            _ackLogPosition3 = Db.Config.WriterCheckpoint.ReadNonFlushed();

            WriteSingleEvent("test-stream", 2, new String('*', 50));
        }

        public override void When()
        {
            var firstEpoch = new Epoch(0, 0, Guid.NewGuid());
            var secondEpoch = new Epoch(Db.Config.WriterCheckpoint.ReadNonFlushed(), 1, Guid.NewGuid());
            var epochs = new Epoch[] { firstEpoch, secondEpoch };
            EpochManager.SetEpochs(epochs);

            Service.Handle(new SystemMessage.SystemStart());
            Service.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid(), _epochId));
            
            var subscription1 = new TestReplicationSubscription(new IPEndPoint(IPAddress.Loopback, 1111));
            var subscription2 = new TestReplicationSubscription(new IPEndPoint(IPAddress.Loopback, 2222));
            var subscription3 = new TestReplicationSubscription(new IPEndPoint(IPAddress.Loopback, 3333));

            var subscribeMsg = subscription1.CreateSubscriptionRequest(MasterId, _subscriptionId1, 0, Guid.NewGuid(), new Epoch[] {firstEpoch});
            Service.Handle(subscribeMsg);
            subscribeMsg = subscription2.CreateSubscriptionRequest(MasterId, _subscriptionId2, 0, Guid.NewGuid(), new Epoch[] {firstEpoch});
            Service.Handle(subscribeMsg);
            subscribeMsg = subscription3.CreateSubscriptionRequest(MasterId, _subscriptionId3, 0, Guid.NewGuid(), new Epoch[] {firstEpoch});
            Service.Handle(subscribeMsg);

            // Ack positions in reverse order
            var replicaAckLogPositionMsg = new ReplicationMessage.ReplicaLogPositionAck(_subscriptionId3, _ackLogPosition3);
            Service.Handle(replicaAckLogPositionMsg);

            replicaAckLogPositionMsg = new ReplicationMessage.ReplicaLogPositionAck(_subscriptionId2, _ackLogPosition2);
            Service.Handle(replicaAckLogPositionMsg);

            replicaAckLogPositionMsg = new ReplicationMessage.ReplicaLogPositionAck(_subscriptionId1, _ackLogPosition1);
            Service.Handle(replicaAckLogPositionMsg);
        }

        [Test]
        public void replication_checkpoint_should_be_set_to_lowest_position_in_quorum()
        {
            Assert.AreEqual(_ackLogPosition2, Db.Config.ReplicationCheckpoint.ReadNonFlushed());
        }
    }
}