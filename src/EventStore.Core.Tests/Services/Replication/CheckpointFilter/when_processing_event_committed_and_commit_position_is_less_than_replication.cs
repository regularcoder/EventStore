using System;
using System.Linq;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Services.Replication;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.TimerService;
using EventStore.Core.Tests.Bus.Helpers;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.LogRecords;
using NUnit.Framework;

namespace EventStore.Core.Tests.Services.Replication.CheckpointFilter
{
    public abstract class with_replication_checkpoint_filter
    {
        protected ReplicationCheckpointFilter _filter;
        protected InMemoryBus _outputBus;
        protected InMemoryBus _publisher;
        protected ICheckpoint _replicationChk;

        protected TestHandler<Message> _outputConsumer;
        protected TestHandler<Message> _publishConsumer;

        [OneTimeSetUp]
        public void SetUp()
        {
            _outputBus = new InMemoryBus("OutputBus");
            _publisher = new InMemoryBus("Publisher");
            _replicationChk = new InMemoryCheckpoint();

            _outputConsumer = new TestHandler<Message>();
            _outputBus.Subscribe(_outputConsumer);

            _publishConsumer = new TestHandler<Message>();
            _publisher.Subscribe(_publishConsumer);

            _filter = new ReplicationCheckpointFilter(_outputBus, _publisher, _replicationChk);

            When();
        }

        public abstract void When();

        protected EventRecord CreateDummyEventRecord(long logPosition)
        {
            return new EventRecord(1, logPosition, Guid.NewGuid(), Guid.NewGuid(), logPosition, 0, "test-stream",
                                   0, DateTime.Now, PrepareFlags.Data, "testEvent", new byte[0], new byte[0]);
        }
    }

    [TestFixture]
    [Category("UnderTest")]
    public class when_processing_event_committed_and_commit_position_is_less_than_replication : with_replication_checkpoint_filter
    {
        public override void When()
        {
            _replicationChk.Write(2000);
            var msg = new StorageMessage.EventCommitted(1000, CreateDummyEventRecord(1000), false);

            _filter.Handle(msg);
        }

        [Test]
        public void should_publish_event_committed_message()
        {
            Assert.AreEqual(1, _outputConsumer.HandledMessages.OfType<StorageMessage.EventCommitted>().Count());
        }
    }

    [TestFixture]
    [Category("UnderTest")]
    public class when_processing_event_committed_and_commit_position_is_greater_than_checkpoint : with_replication_checkpoint_filter
    {
        private TimerMessage.Schedule _scheduledMessage;

        public override void When()
        {
            _replicationChk.Write(2000);
            var msg = new StorageMessage.EventCommitted(3000, CreateDummyEventRecord(3000), false);
            _scheduledMessage = _publishConsumer.HandledMessages.OfType<TimerMessage.Schedule>().SingleOrDefault();

            _filter.Handle(msg);
        }

        [Test]
        public void should_not_publish_event_committed_message()
        {
            Assert.AreEqual(0, _outputConsumer.HandledMessages.OfType<StorageMessage.EventCommitted>().Count());
        }

        [Test]
        public void should_schedule_a_replication_checkpoint_check_message()
        {
            Assert.IsNotNull(_scheduledMessage);
            Assert.IsInstanceOf<ReplicationMessage.ReplicationAckCheckTick>(_scheduledMessage.ReplyMessage);
        }

        [Test]
        public void should_publish_event_committed_message_on_tick_after_checkpoint_increases()
        {
            _replicationChk.Write(4000);
            _filter.Handle((ReplicationMessage.ReplicationAckCheckTick)_scheduledMessage.ReplyMessage);
            Assert.AreEqual(1, _outputConsumer.HandledMessages.OfType<StorageMessage.EventCommitted>().Count());
        }
    }

    [TestFixture]
    [Category("UnderTest")]
    public class when_processing_multiple_event_committed_messages : with_replication_checkpoint_filter
    {
        private TimerMessage.Schedule _scheduledMessage;

        public override void When()
        {
            _scheduledMessage = _publishConsumer.HandledMessages.OfType<TimerMessage.Schedule>().SingleOrDefault();
            _replicationChk.Write(-1);

            var msg1 = new StorageMessage.EventCommitted(1000, CreateDummyEventRecord(1000), false);
            var msg2 = new StorageMessage.EventCommitted(2000, CreateDummyEventRecord(2000), false);
            var msg3 = new StorageMessage.EventCommitted(3000, CreateDummyEventRecord(3000), false);
            _filter.Handle(msg1);
            _filter.Handle(msg2);
            _filter.Handle(msg3);

            _replicationChk.Write(2000);
            _filter.Handle((ReplicationMessage.ReplicationAckCheckTick)_scheduledMessage.ReplyMessage);
        }

        [Test]
        public void should_publish_event_committed_message_on_tick_for_first_two_messages()
        {
            var messages = _outputConsumer.HandledMessages.OfType<StorageMessage.EventCommitted>().ToList();
            Assert.AreEqual(2, messages.Count);
            Assert.IsTrue(messages.Any(x=> ((StorageMessage.EventCommitted)x).CommitPosition == 1000));
            Assert.IsTrue(messages.Any(x=> ((StorageMessage.EventCommitted)x).CommitPosition == 2000));
        }

        [Test]
        public void should_publish_third_event_on_tick_after_checkpoint_increases()
        {
            _outputConsumer.HandledMessages.Clear();
            _replicationChk.Write(3000);
            _filter.Handle((ReplicationMessage.ReplicationAckCheckTick)_scheduledMessage.ReplyMessage);
            Assert.AreEqual(1, _outputConsumer.HandledMessages.OfType<StorageMessage.EventCommitted>().Count());
        }
    }

    [TestFixture]
    [Category("UnderTest")]
    public class when_processing_multiple_event_committed_messages_for_the_same_positions : with_replication_checkpoint_filter
    {
        private TimerMessage.Schedule _scheduledMessage;

        public override void When()
        {
            _scheduledMessage = _publishConsumer.HandledMessages.OfType<TimerMessage.Schedule>().SingleOrDefault();
            _replicationChk.Write(-1);

            var msg1 = new StorageMessage.EventCommitted(1000, CreateDummyEventRecord(1000), false);
            var msg2 = new StorageMessage.EventCommitted(1000, CreateDummyEventRecord(1000), false);
            _filter.Handle(msg1);
            _filter.Handle(msg2);

            _replicationChk.Write(2000);
            _filter.Handle((ReplicationMessage.ReplicationAckCheckTick)_scheduledMessage.ReplyMessage);
        }

        [Test]
        public void should_publish_event_committed_message_for_both_messages()
        {
            var messages = _outputConsumer.HandledMessages.OfType<StorageMessage.EventCommitted>().ToList();
            Assert.AreEqual(2, messages.Count);
        }
    }
}