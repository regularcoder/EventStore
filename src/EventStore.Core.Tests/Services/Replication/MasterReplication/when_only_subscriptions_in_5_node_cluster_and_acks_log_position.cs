using System;
using System.Net;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Replication.MasterReplication
{
    [TestFixture]
    [Category("UnderTest")]
    public class when_only_subscriptions_in_5_node_cluster_and_acks_log_position : with_master_replication_service
    {
        private long _ackLogPosition;

        private Guid _subscriptionId = Guid.NewGuid();
        private Guid _epochId = Guid.NewGuid();

        public when_only_subscriptions_in_5_node_cluster_and_acks_log_position()
        {
            ClusterSize = 5;

        }

        public override void WriteTestScenario()
        {
            WriteSingleEvent("test-stream", 0, new String('*', 50));
            WriteSingleEvent("test-stream", 1, new String('*', 50));
            WriteSingleEvent("test-stream", 2, new String('*', 50));
            _ackLogPosition = Db.Config.WriterCheckpoint.ReadNonFlushed();
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

            var subscribeMsg = subscription1.CreateSubscriptionRequest(MasterId, _subscriptionId, 0, Guid.NewGuid(), new Epoch[] {firstEpoch});
            Service.Handle(subscribeMsg);

            var replicaAckLogPositionMsg = new ReplicationMessage.ReplicaLogPositionAck(_subscriptionId, _ackLogPosition);
            Service.Handle(replicaAckLogPositionMsg);
        }

        [Test]
        public void replication_checkpoint_should_not_change()
        {
            Assert.AreEqual(-1, Db.Config.ReplicationCheckpoint.ReadNonFlushed());
        }
    }
}