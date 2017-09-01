using System;
using System.Net;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Replication.MasterReplication
{
    [TestFixture]
    [Category("UnderTest")]
    public class when_replica_subscribes_from_a_position_but_is_not_caught_up : with_master_replication_service
    {
        private long _subscribePosition;
        private Guid _epochId = Guid.NewGuid();

        public override void WriteTestScenario()
        {
            WriteSingleEvent("test-stream", 0, new String('*', 50));
            _subscribePosition = Db.Config.WriterCheckpoint.ReadNonFlushed();

            WriteSingleEvent("test-stream", 1, new String('*', 50));
            WriteSingleEvent("test-stream", 2, new String('*', 50));
        }

        public override void When()
        {
            var firstEpoch = new Epoch(0, 0, Guid.NewGuid());
            var epochs = new Epoch[] { firstEpoch, new Epoch(Db.Config.WriterCheckpoint.ReadNonFlushed(), 1, Guid.NewGuid()) };
            EpochManager.SetEpochs(epochs);

            Service.Handle(new SystemMessage.SystemStart());
            Service.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid(), _epochId));
            
            var subscription = new TestReplicationSubscription(new IPEndPoint(IPAddress.Loopback, 1111));
            var subscribeMsg = subscription.CreateSubscriptionRequest(MasterId, Guid.NewGuid(), _subscribePosition, Guid.NewGuid(), new Epoch[] {firstEpoch});
            Service.Handle(subscribeMsg);
        }

        [Test]
        public void replication_checkpoint_should_have_been_updated()
        {
            Assert.AreEqual(_subscribePosition, Db.Config.ReplicationCheckpoint.ReadNonFlushed());
        }
    }
}