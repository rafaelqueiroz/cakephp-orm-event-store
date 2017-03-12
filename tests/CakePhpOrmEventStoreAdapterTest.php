<?php

namespace ProophTest\EventStore\Adapter\CakePHP;

use Cake\Database\Connection;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\CakePHP\CakePhpOrmEventStoreAdapter;
use Prooph\EventStore\Adapter\PayloadSerializer\JsonPayloadSerializer;
use Prooph\EventStore\Stream\StreamName;
use ProophTest\EventStore\Adapter\CakePHP\Mock\UsernameWasChanged;
use ProophTest\EventStore\Adapter\CakePHP\Mock\UserWasCreated;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Exception\RuntimeException;

class CakePhpOrmEventStoreAdapterTest extends TestCase
{

    /**
     * @var CakePhpOrmEventStoreAdapter $adapter
     */
    private $adapter;

    public function setUp()
    {
        $config = [
            'className' => 'Cake\Database\Connection',
            'driver' => 'Cake\Database\Driver\Sqlite',
            'db' => ':memory:'
        ];

        $this->adapter = new CakePhpOrmEventStoreAdapter(
            new Connection($config),
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            New JsonPayloadSerializer()
        );
    }

    /**
     * @test
     */
    public function it_creates_a_stream()
    {
        $testStream = $this->getTestStream();
        $this->adapter->beginTransaction();
        $this->adapter->create($testStream);
        $this->adapter->commit();
        $streamEvents = $this->adapter->loadEvents(new StreamName('CakePHP\Model\User'), ['tag' => 'person']);

        $count = 0;
        foreach ($streamEvents as $event) {
            $count++;
        }

        $this->assertEquals(1, $count);
        $testStream->streamEvents()->rewind();
        $testEvent = $testStream->streamEvents()->current();
        $this->assertEquals($testEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($testEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals(UserWasCreated::class, $event->messageName());
        $this->assertEquals('luciiano.queiroz@gmail.com', $event->payload()['email']);
        $this->assertEquals(1, $event->version());
        $this->assertEquals(['tag' => 'person'], $event->metadata());
    }

    /**
     * @test
     */
    public function it_appends_events_to_a_stream()
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent = UsernameWasChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('CakePHP\Model\User'), new \ArrayIterator([$streamEvent]));
        $stream = $this->adapter->load(new StreamName('CakePHP\Model\User'), null, $streamEvent->metadata());

        $this->assertEquals('CakePHP\Model\User', $stream->streamName()->toString());

        $count = 0;
        $lastEvent = null;
        foreach ($stream->streamEvents() as $event) {
            $count++;
            $lastEvent = $event;
        }

        $this->assertEquals(2, $count);
        $this->assertInstanceOf(UsernameWasChanged::class, $lastEvent);
        $messageConverter = new NoOpMessageConverter();

        $streamEventData = $messageConverter->convertToArray($streamEvent);
        $lastEventData = $messageConverter->convertToArray($lastEvent);
        $this->assertEquals($streamEventData, $lastEventData);
    }

    /**
     * @test
     */
    public function it_can_return_sql_string_for_schema_creation()
    {
        $sqls = $this->adapter->createSchemaFor(new StreamName('CakePHP\Model\User'), [], true);
        $this->assertInternalType('array', $sqls);
        $this->assertArrayHasKey(0, $sqls);
        $this->assertInternalType('string', $sqls[0]);
    }

    /**
     * @test
     */
    public function test_it_exists()
    {
        $this->assertInstanceOf(CakePhpOrmEventStoreAdapter::class, $this->adapter);
    }

    /**
     * @test
     */
    public function it_injected_correct_db_connection()
    {
        $connectionMock = $this->getMockForAbstractClass('Cake\Database\Connection', [], '', false);
        $adapter = new CakePhpOrmEventStoreAdapter(
            $connectionMock,
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            new JsonPayloadSerializer()
        );
        $this->assertSame($connectionMock, $adapter->getConnection());
    }

    /**
     * @return Stream
     */
    private function getTestStream()
    {
        $streamEvent = UserWasCreated::with(
            ['name' => 'Luciano Queiroz', 'email' => 'luciiano.queiroz@gmail.com'],
            1
        );
        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        return new Stream(new StreamName('CakePHP\Model\User'), new \ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     */
    public function it_can_rewind_cakephp_orm_stream_iterator()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $result = $this->adapter->loadEvents(new StreamName('CakePHP\Model\User'), ['tag' => 'person']);

        $this->assertNotNull($result->current());
        $this->assertEquals(0, $result->key());
        $result->next();
        $this->assertNull($result->current());

        $result->rewind();
        $this->assertNotNull($result->current());
        $this->assertEquals(0, $result->key());
        $result->next();
        $this->assertNull($result->current());
        $this->assertFalse($result->key());
    }

    /**
     * @test
     * @expectedException \PDOException
     */
    public function it_fails_to_write_with_duplicate_aggregate_id_and_version()
    {
        $streamEvent = UserWasCreated::with(
            ['name' => 'Luciano Queiroz', 'email' => 'luciiano.queiroz@gmail.com'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('aggregate_id', 'one');
        $streamEvent = $streamEvent->withAddedMetadata('aggregate_type', 'user');

        $this->adapter->create(new Stream(new StreamName('CakePHP\Model\User'), new \ArrayIterator([$streamEvent])));

        $streamEvent = UsernameWasChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('aggregate_id', 'one');
        $streamEvent = $streamEvent->withAddedMetadata('aggregate_type', 'user');

        $this->adapter->appendTo(new StreamName('CakePHP\Model\User'), new \ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     */
    public function it_replays_larger_streams_in_chunks()
    {
        $streamName = new StreamName('CakePHP\Model\User');

        $streamEvents = [];

        for ($i = 1; $i <= 150; $i++) {
            $streamEvents[] = UserWasCreated::with(
                ['name' => 'Luciano Queiroz ' . $i, 'email' => 'luciiano.queiroz_' . $i . '@gmail.com'],
                $i
            );
        }

        $this->adapter->create(new Stream($streamName, new \ArrayIterator($streamEvents)));

        $replay = $this->adapter->replay($streamName);

        $count = 0;
        foreach ($replay as $event) {
            $count++;
            $this->assertEquals('Luciano Queiroz ' . $count, $event->payload()['name']);
            $this->assertEquals('luciiano.queiroz_' . $count . '@gmail.com', $event->payload()['email']);
            $this->assertEquals($count, $event->version());
        }

        $this->assertEquals(150, $count);
    }

    /**
     * @test
     */
    public function it_can_rollback_transaction()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $this->adapter->beginTransaction();

        $streamEvent = UserWasCreated::with(
            ['name' => 'Luciano Queiroz', 'email' => 'luciiano.queiroz@gmail.com'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('CakePHP\Model\User'), new \ArrayIterator([$streamEvent]));

        $this->adapter->rollback();

        $result = $this->adapter->loadEvents(new StreamName('CakePHP\Model\User'), ['tag' => 'person']);

        $this->assertNotNull($result->current());
        $this->assertEquals(0, $result->key());
        $result->next();
        $this->assertNull($result->current());
    }

    /**
     * @test
     * @expectedException RuntimeException
     * @expectedExceptionMessage Cannot create empty stream CakePHP\Model\User.
     */
    public function it_throws_exception_when_empty_stream_created()
    {
        $this->adapter->create(new Stream(new StreamName('CakePHP\Model\User'), new \ArrayIterator([])));
    }

    /**
     * @test
     * @expectedException RuntimeException
     * @expectedExceptionMessage Transaction already started
     */
    public function it_throws_exception_when_second_transaction_started()
    {
        $this->adapter->beginTransaction();
        $this->adapter->beginTransaction();
    }

    /**
     * @test
     */
    public function it_replays_events_of_two_aggregates_in_a_single_stream_in_correct_order()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $streamEvent = UsernameWasChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('CakePHP\Model\User'), new \ArrayIterator([$streamEvent]));

        sleep(1);

        $secondUserEvent = UserWasCreated::with(
            ['name' => 'Jane Doe', 'email' => 'jane@acme.com'],
            1
        );

        $secondUserEvent = $secondUserEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('CakePHP\Model\User'), new \ArrayIterator([$secondUserEvent]));

        $streamEvents = $this->adapter->replay(new StreamName('CakePHP\Model\User'), null, ['tag' => 'person']);


        $replayedPayloads = [];
        foreach ($streamEvents as $event) {
            $replayedPayloads[] = $event->payload();
        }

        $expectedPayloads = [
            ['name' => 'Luciano Queiroz', 'email' => 'luciiano.queiroz@gmail.com'],
            ['name' => 'John Doe'],
            ['name' => 'Jane Doe', 'email' => 'jane@acme.com'],
        ];

        $this->assertEquals($expectedPayloads, $replayedPayloads);
    }

    /**
     * @test
     */
    public function it_replays_from_specific_date()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        sleep(1);

        $since = new \DateTime('now', new \DateTimeZone('UTC'));

        $streamEvent = UsernameWasChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('CakePHP\Model\User'), new \ArrayIterator([$streamEvent]));

        $streamEvents = $this->adapter->replay(new StreamName('CakePHP\Model\User'), $since, ['tag' => 'person']);

        $count = 0;
        foreach ($streamEvents as $event) {
            $count++;
        }
        $this->assertEquals(1, $count);

        $testStream->streamEvents()->rewind();
        $streamEvents->rewind();

        $event = $streamEvents->current();

        $this->assertEquals($streamEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($streamEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Adapter\CakePHP\Mock\UsernameWasChanged', $event->messageName());
        $this->assertEquals('John Doe', $event->payload()['name']);
        $this->assertEquals(2, $event->version());
    }

    /**
     * @test
     */
    public function it_replays()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $streamEvent = UsernameWasChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('CakePHP\Model\User'), new \ArrayIterator([$streamEvent]));

        $streamEvents = $this->adapter->replay(new StreamName('CakePHP\Model\User'), null, ['tag' => 'person']);

        $count = 0;
        foreach ($streamEvents as $event) {
            $count++;
        }
        $this->assertEquals(2, $count);

        $testStream->streamEvents()->rewind();
        $streamEvents->rewind();

        $testEvent = $testStream->streamEvents()->current();
        $event = $streamEvents->current();

        $this->assertEquals($testEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($testEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Adapter\CakePHP\Mock\UserWasCreated', $event->messageName());
        $this->assertEquals('luciiano.queiroz@gmail.com', $event->payload()['email']);
        $this->assertEquals(1, $event->version());

        $streamEvents->next();
        $event = $streamEvents->current();

        $this->assertEquals($streamEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($streamEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Adapter\CakePHP\Mock\UsernameWasChanged', $event->messageName());
        $this->assertEquals('John Doe', $event->payload()['name']);
        $this->assertEquals(2, $event->version());
    }

    /**
     * @test
     */
    public function it_loads_events_from_min_version_on()
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent1 = UsernameWasChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent1 = $streamEvent1->withAddedMetadata('tag', 'person');


        $streamEvent2 = UsernameWasChanged::with(
            ['name' => 'Jane Doe'],
            3
        );

        $streamEvent2 = $streamEvent2->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('CakePHP\Model\User'), new \ArrayIterator([$streamEvent1, $streamEvent2]));

        $stream = $this->adapter->load(new StreamName('CakePHP\Model\User'), 2);

        $this->assertEquals('CakePHP\Model\User', $stream->streamName()->toString());

        $event1 = $stream->streamEvents()->current();
        $stream->streamEvents()->next();
        $event2 = $stream->streamEvents()->current();
        $stream->streamEvents()->next();
        $this->assertFalse($stream->streamEvents()->valid());

        $this->assertEquals('John Doe', $event1->payload()['name']);
        $this->assertEquals('Jane Doe', $event2->payload()['name']);
    }
}
