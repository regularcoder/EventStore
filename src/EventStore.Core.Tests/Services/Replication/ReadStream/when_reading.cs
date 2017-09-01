using System;
using System.Linq;
using System.Threading;
using EventStore.Core.Bus;
using EventStore.Core.Tests.Helpers;
using NUnit.Framework;
using EventStore.Core.Tests.Integration;
using EventStore.Core.Services.UserManagement;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services;
using EventStore.Core.Tests.Services.Replication;
using EventStore.Core.Data;

namespace EventStore.Core.Tests.Replication.ReadStream
{
    [TestFixture, Category("UnderTest")]
    [Ignore("Unfinished")]
    public class when_reading_an_event_committed_on_master_and_not_on_slaves : specification_with_cluster
    {
        private CountdownEvent _expectedNumberOfRoleAssignments;
        private ClientMessage.ReadEventCompleted _readResult;

        protected override void BeforeNodesStart()
        {
            _nodes.ToList().ForEach(x =>
                x.Node.MainBus.Subscribe(new AdHocHandler<SystemMessage.StateChangeMessage>(Handle)));
            _expectedNumberOfRoleAssignments = new CountdownEvent(3);
            base.BeforeNodesStart();
        }

        protected override void Given()
        {
            _expectedNumberOfRoleAssignments.Wait(5000);
            var master = _nodes.First(x => x.NodeState == Data.VNodeState.Master);
            // Write event
            var events = new Event[]{new Event(Guid.NewGuid(), "test-type", false, new byte[10], new byte[0]) };
            master.Node.MainQueue.Publish(new ClientMessage.WriteEvents(Guid.NewGuid(), Guid.NewGuid(), 
                                          new FakeEnvelope(), true, "test-stream", -1, events, 
                                          SystemAccount.Principal, SystemUsers.Admin, SystemUsers.DefaultAdminPassword));
            // Don't replicate to slaves? How?
            var read = new ClientMessage.ReadEvent(Guid.NewGuid(), Guid.NewGuid(), new CallbackEnvelope(x => _readResult = (ClientMessage.ReadEventCompleted)x), "test-stream", 0,
                             false, true, SystemAccount.Principal);
            master.Node.MainQueue.Publish(read);
            base.Given();
        }

        private void Handle(SystemMessage.StateChangeMessage msg)
        {
            switch (msg.State)
            {
                case Data.VNodeState.Master:
                    _expectedNumberOfRoleAssignments.Signal();
                    break;
                case Data.VNodeState.Slave:
                    _expectedNumberOfRoleAssignments.Signal();
                    break;
            }
        }

        [Test]
        public void should_not_be_able_to_read_event()
        {
            Assert.AreEqual(ReadEventResult.NotFound, _readResult.Result);
        } 
    }
}