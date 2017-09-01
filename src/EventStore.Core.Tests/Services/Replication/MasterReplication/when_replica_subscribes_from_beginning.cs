using System;
using System.Net;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Replication.MasterReplication
{
    [TestFixture]
    public class when_replica_subscribes_from_beginning : with_master_replication_service
    {
        private long _subscribePosition;
        private Guid _epochId = Guid.NewGuid();

        public override void WriteTestScenario()
        {
            WriteSingleEvent("test-stream", 0, new String('*', 50));
            WriteSingleEvent("test-stream", 1, new String('*', 50));
            WriteSingleEvent("test-stream", 2, new String('*', 50));
        }

        public override void When()
        {
            _subscribePosition = 0;

            Service.Handle(new SystemMessage.SystemStart());
            Service.Handle(new SystemMessage.BecomeMaster(Guid.NewGuid(), _epochId));

            var subscription = new TestReplicationSubscription(new IPEndPoint(IPAddress.Loopback, 1111));
            var subscribeMsg = subscription.CreateSubscriptionRequest(MasterId, Guid.NewGuid(), _subscribePosition, Guid.NewGuid(), new Epoch[0]);
            Service.Handle(subscribeMsg);
        }

        [Test]
        public void replication_checkpoint_should_have_been_updated()
        {
            Assert.AreEqual(_subscribePosition, Db.Config.ReplicationCheckpoint.ReadNonFlushed());
        }
    }
}