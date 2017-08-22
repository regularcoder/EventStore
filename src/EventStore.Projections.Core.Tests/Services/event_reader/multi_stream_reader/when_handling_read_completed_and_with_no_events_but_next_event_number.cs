using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Services.AwakeReaderService;
using EventStore.Core.Services.TimerService;
using EventStore.Core.TransactionLog.LogRecords;
using EventStore.Projections.Core.Services.Processing;
using EventStore.Projections.Core.Tests.Services.core_projection;
using NUnit.Framework;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;

namespace EventStore.Projections.Core.Tests.Services.event_reader.multi_stream_reader
{
    [TestFixture]
    public class when_handling_read_completed_and_with_no_events_but_next_event_number : TestFixtureWithExistingEvents
    {
        private MultiStreamEventReader _edp;
        private Guid _distibutionPointCorrelationId;
        private Guid _firstEventId;
        private Guid _secondEventId;

        protected override void Given()
        {
            TicksAreHandledImmediately();
        }

        private string[] _abStreams;
        private Dictionary<string, long> _ab12Tag;

        [SetUp]
        public new void When()
        {
            _ab12Tag = new Dictionary<string, long> {{"a", 1}, {"b", 0}};
            _abStreams = new[] {"a", "b"};

            _distibutionPointCorrelationId = Guid.NewGuid();
            _edp = new MultiStreamEventReader(
                _ioDispatcher, _bus, _distibutionPointCorrelationId, null, 0, _abStreams, _ab12Tag, false,
                new RealTimeProvider());
            _edp.Resume();
            _firstEventId = Guid.NewGuid();
            _secondEventId = Guid.NewGuid();
            var correlationId = _consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Last(x => x.EventStreamId == "a").CorrelationId;
            _edp.Handle(
                new ClientMessage.ReadStreamEventsForwardCompleted(
                correlationId, "a", 1, 100, ReadStreamResult.Success,
                    new[]
                        {
                            ResolvedEvent.ForUnresolvedEvent(

                        new EventRecord(
                            1, 50, Guid.NewGuid(), _firstEventId, 50, 0, "a", ExpectedVersion.Any, DateTime.UtcNow,
                            PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
                            "event_type1", new byte[] {1}, new byte[] {2})),
                            ResolvedEvent.ForUnresolvedEvent(
                        new EventRecord(
                            2, 100, Guid.NewGuid(), _secondEventId, 100, 0, "a", ExpectedVersion.Any, DateTime.UtcNow,
                            PrepareFlags.SingleWrite | PrepareFlags.TransactionBegin | PrepareFlags.TransactionEnd,
                            "event_type2", new byte[] {3}, new byte[] {4}))
                        }, null, false, "", 3, 2, true, 200));
            correlationId = _consumer.HandledMessages.OfType<ClientMessage.ReadStreamEventsForward>().Last(x => x.EventStreamId == "b").CorrelationId;
            _edp.Handle(
                new ClientMessage.ReadStreamEventsForwardCompleted(
                    correlationId, "b", 0, 10, ReadStreamResult.Success, new ResolvedEvent[0], new StreamMetadata(truncateBefore: 10), false, "",
                    10, ExpectedVersion.NoStream, false, 200));
        }

        [Test]
        public void it_should_issue_the_next_read_from_the_next_event_number()
        {
            var replyMessage = (ClientMessage.ReadStreamEventsForward)_consumer.HandledMessages.OfType<AwakeServiceMessage.SubscribeAwake>()
                         .Last(m => m.ReplyWithMessage is ClientMessage.ReadStreamEventsForward && ((ClientMessage.ReadStreamEventsForward)m.ReplyWithMessage).EventStreamId == "b").ReplyWithMessage;
            Assert.AreEqual(10, replyMessage.FromEventNumber);
        }
    }
}
