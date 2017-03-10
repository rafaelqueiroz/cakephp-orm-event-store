<?php

namespace ProophTest\EventStore\Adapter\CakePHP;

use Cake\Database\Connection;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\CakePHP\CakePhpOrmEventStoreAdapter;
use Prooph\EventStore\Adapter\PayloadSerializer\JsonPayloadSerializer;

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

    public function test_it_exists()
    {
        $this->assertInstanceOf(CakePhpOrmEventStoreAdapter::class, $this->adapter);
    }

    /**
     * @test
     */
    public function it_injected_correct_db_connection()
    {
        $connectionMock = $this->getMockForAbstractClass('Cake\Datasource\ConnectionInterface', [], '', false);
        $adapter = new CakePhpOrmEventStoreAdapter(
            $connectionMock,
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            new JsonPayloadSerializer()
        );
        $this->assertSame($connectionMock, $adapter->getConnection());
    }
}