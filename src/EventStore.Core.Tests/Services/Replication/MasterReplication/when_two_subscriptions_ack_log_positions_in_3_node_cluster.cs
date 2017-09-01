using System;
using System.Net;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Replication.MasterReplication
{
    [TestFixture]
    [Category("UnderTest")]
    public class when_two_subscriptions_ack_log_positions_in_3_node_cluster : with_master_replication_service
    {
        private long _ackLogPosition1;
        private long _ackLogPosition2;

        private Guid _subscriptionId1 = Guid.NewGuid();
        private Guid _subscriptionId2 = Guid.NewGuid();
        private Guid _epochId = Guid.NewGuid();

        public override void WriteTestScenario()
        {
            WriteSingleEvent("test-stream", 0, new String('*', 50));
            _ackLogPosition1 = Db.Config.WriterCheckpoint.ReadNonFlushed();

            WriteSingleEvent("test-stream", 1, new String('*', 50));
            _ackLogPosition2 = Db.Config.WriterCheckpoint.ReadNonFlushed();

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
            var subscribeMsg = subscription1.CreateSubscriptionRequest(MasterId, _subscriptionId1, 0, Guid.NewGuid(), new Epoch[] {firstEpoch});
            Service.Handle(subscribeMsg);

            var subscription2 = new TestReplicationSubscription(new IPEndPoint(IPAddress.Loopback, 1111));
            subscribeMsg = subscription2.CreateSubscriptionRequest(MasterId, _subscriptionId2, 0, Guid.NewGuid(), new Epoch[] {firstEpoch});
            Service.Handle(subscribeMsg);

            var replicaAckLogPositionMsg = new ReplicationMessage.ReplicaLogPositionAck(_subscriptionId2, _ackLogPosition2);
            Service.Handle(replicaAckLogPositionMsg);

            replicaAckLogPositionMsg = new ReplicationMessage.ReplicaLogPositionAck(_subscriptionId1, _ackLogPosition1);
            Service.Handle(replicaAckLogPositionMsg);
        }

        [Test]
        public void replication_checkpoint_should_be_updated_to_highest_acked_log_position()
        {
            Assert.AreEqual(_ackLogPosition2, Db.Config.ReplicationCheckpoint.ReadNonFlushed());
        }
    }
}