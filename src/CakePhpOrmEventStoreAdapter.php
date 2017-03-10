<?php

namespace Prooph\EventStore\Adapter\CakePHP;

use Cake\Datasource\ConnectionInterface;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Stream\StreamName;
use Prooph\EventStore\Stream\Stream;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\PayloadSerializer\JsonPayloadSerializer;
use Iterator;
use DateTimeInterface;

class CakePhpOrmEventStoreAdapter implements Adapter
{
    /**
     * @var ConnectionInterface $connection
     */
    private $connection;
    /**
     * @var FQCNMessageFactory $messageFactory
     */
    private $messageFactory;
    /**
     * @var NoOpMessageConverter $messageConverter
     */
    private $messageConverter;
    /**
     * @var JsonPayloadSerializer $payloadSerializer
     */
    private $payloadSerializer;

    /**
     * CakePhpOrmEventStoreAdapter constructor.
     *
     * @param ConnectionInterface $connection
     * @param FQCNMessageFactory $messageFactory
     * @param NoOpMessageConverter $messageConverter
     * @param JsonPayloadSerializer $payloadSerializer
     */
    public function __construct(
        ConnectionInterface $connection,
        FQCNMessageFactory $messageFactory,
        NoOpMessageConverter $messageConverter,
        JsonPayloadSerializer $payloadSerializer
    ) {
        $this->connection = $connection;
        $this->messageFactory = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->payloadSerializer = $payloadSerializer;
    }

    /**
     * @return ConnectionInterface
     */
    public function getConnection()
    {
        return $this->connection;
    }

    public function load(StreamName $streamName, $minVersion = null)
    {
        // TODO: Implement load() method.
    }

    public function create(Stream $stream)
    {
        // TODO: Implement create() method.
    }

    public function appendTo(StreamName $streamName, Iterator $domainEvents)
    {
        // TODO: Implement appendTo() method.
    }

    public function replay(StreamName $streamName, DateTimeInterface $since = null, array $metadata = [])
    {
        // TODO: Implement replay() method.
    }

    public function loadEvents(StreamName $streamName, array $metadata = [], $minVersion = null)
    {
        // TODO: Implement loadEvents() method.
    }
}