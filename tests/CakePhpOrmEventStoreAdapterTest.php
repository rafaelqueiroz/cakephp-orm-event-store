<?php

namespace ProophTest\EventStore\Adapter\CakePHP;

use Cake\Database\Connection;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\CakePHP\CakePhpOrmEventStoreAdapter;
use Prooph\EventStore\Adapter\PayloadSerializer\JsonPayloadSerializer;
use Prooph\EventStore\Stream\StreamName;
use ProophTest\EventStore\Adapter\CakePHP\Mock\UserWasCreated;
use Prooph\EventStore\Stream\Stream;

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
    public function it_can_return_sql_string_for_schema_creation()
    {
        $sqls = $this->adapter->createSchemaFor(new StreamName('CakePHP\Model\User'), [], true);
        $this->assertInternalType('array', $sqls);
        $this->assertArrayHasKey(0, $sqls);
        $this->assertInternalType('string', $sqls[0]);
    }

    /**
     *
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
}