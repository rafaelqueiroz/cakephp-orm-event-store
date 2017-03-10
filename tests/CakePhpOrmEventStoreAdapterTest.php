<?php

namespace ProophTest\EventStore\Adapter\CakePHP;

use Cake\Database\Connection;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\CakePHP\CakePhpOrmEventStoreAdapter;
use Prooph\EventStore\Adapter\PayloadSerializer\JsonPayloadSerializer;
use Prooph\EventStore\Stream\StreamName;

class CakePhpOrmEventStoreAdapterTest extends TestCase
{

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
}