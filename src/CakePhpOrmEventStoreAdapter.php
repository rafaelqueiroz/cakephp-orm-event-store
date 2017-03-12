<?php

namespace Prooph\EventStore\Adapter\CakePHP;

use Cake\Database\Connection;
use Cake\Datasource\ConnectionInterface;
use Cake\Database\Schema\TableSchema;
use Cake\ORM\Query;
use Cake\ORM\Table;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Stream\StreamName;
use Prooph\EventStore\Stream\Stream;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\PayloadSerializer\JsonPayloadSerializer;
use Iterator;
use Prooph\Common\Messaging\MessageDataAssertion;
use Prooph\Common\Messaging\Message;

class CakePhpOrmEventStoreAdapter implements Adapter
{
    /**
     * Custom sourceType to table mapping
     *
     * @var array
     */
    private $streamTableMap = [];
    /**
     * @var Connection $connection
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
     * @var int
     */
    private $loadBatchSize;

    /**
     * CakePhpOrmEventStoreAdapter constructor.
     *
     * @param Connection $connection
     * @param FQCNMessageFactory $messageFactory
     * @param NoOpMessageConverter $messageConverter
     * @param JsonPayloadSerializer $payloadSerializer
     */
    public function __construct(
        Connection $connection,
        FQCNMessageFactory $messageFactory,
        NoOpMessageConverter $messageConverter,
        JsonPayloadSerializer $payloadSerializer,
        array $streamTableMap = [],
        $loadBatchSize = 10000
    ) {
        $this->connection = $connection;
        $this->messageFactory = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->payloadSerializer = $payloadSerializer;
        $this->streamTableMap = $streamTableMap;
        $this->loadBatchSize = $loadBatchSize;
    }

    /**
     * @return ConnectionInterface
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * @param StreamName $streamName
     * @param array $metadata
     * @param bool $returnSql
     * @return array|void If $returnSql is set to true then method returns array of SQL strings
     */
    public function createSchemaFor(StreamName $streamName, array $metadata, $returnSql = false)
    {
        $tableSchema = new TableSchema($this->getTable($streamName));
        static::addToTableSchema($tableSchema, $metadata);
        $sqls = $tableSchema->createSql($this->connection);
        if ($returnSql) {
            return $sqls;
        }
        foreach ($sqls as $sql) {
            $this->connection->execute($sql);
        }
    }

    /**
     * @param Schema $schema
     * @param string $table
     * @param array $metadata
     */
    public static function addToTableSchema(TableSchema $tableSchema, array $metadata)
    {
        $tableSchema->addColumn('event_id', ['type' => 'string', 'length' => 36]);
        $tableSchema->addColumn('version', ['type' => 'integer']);
        $tableSchema->addColumn('event_name', ['type' => 'string', 'length' => 100]);
        $tableSchema->addColumn('payload', ['type' => 'text']);
        $tableSchema->addColumn('created_at', ['type' => 'string', 'length' => 50]);
        foreach ($metadata as $key => $value) {
            $tableSchema->addColumn($key,['type' => 'string', 'length' => 100]);
        }
        if ($tableSchema->column('aggregate_id')) {
            $tableSchema->addConstraint('aggregate_id', ['columns' => 'aggregate_id', 'type' => 'unique']);
            $tableSchema->addConstraint('version', ['columns' => 'version', 'type' => 'unique']);
        }
        $tableSchema->addConstraint('event_id',['columns' => 'event_id','type' => 'primary']);
    }

    /**
     * Get table name for given stream name
     *
     * @param StreamName $streamName
     * @return string
     */
    public function getTable(StreamName $streamName)
    {
        if (isset($this->streamTableMap[$streamName->toString()])) {
            $tableName = $this->streamTableMap[$streamName->toString()];
        } else {
            $tableName = strtolower($this->getShortStreamName($streamName));
            if (strpos($tableName, "_stream") === false) {
                $tableName.= "_stream";
            }
        }
        return $tableName;
    }

    /**
     * @param StreamName $streamName
     * @return string
     */
    private function getShortStreamName(StreamName $streamName)
    {
        $streamName = str_replace('-', '_', $streamName->toString());
        return implode('', array_slice(explode('\\', $streamName), -1));
    }

    /**
     *
     */
    public function beginTransaction()
    {
        if ($this->connection->inTransaction()) {
            throw new \RuntimeException('Transaction already started');
        }
        $this->connection->begin();
    }

    /**
     * @throws null
     */
    public function commit()
    {
        $this->connection->commit();
    }

    /**
     *
     */
    public function rollback()
    {
        $this->connection->rollback();
    }

    /**
     * @param StreamName $streamName
     * @param null $minVersion
     * @param array $metadata
     * @return Stream
     */
    public function load(StreamName $streamName, $minVersion = null, $metadata = [])
    {
        $events = $this->loadEvents($streamName, $metadata, $minVersion);

        return new Stream($streamName, $events);
    }

    /**
     * @param Stream $stream
     * @throws ConcurrencyException
     */
    public function create(Stream $stream)
    {
        if (!$stream->streamEvents()->valid()) {
            throw new \RuntimeException(
                sprintf(
                    "Cannot create empty stream %s. %s requires at least one event to extract metadata information",
                    $stream->streamName()->toString(),
                    __CLASS__
                )
            );
        }

        $firstEvent = $stream->streamEvents()->current();
        $this->createSchemaFor($stream->streamName(), $firstEvent->metadata());

        $this->appendTo($stream->streamName(), $stream->streamEvents());
    }

    /**
     * @param StreamName $streamName
     * @param Iterator $streamEvents
     * @throws ConcurrencyException
     */
    public function appendTo(StreamName $streamName, Iterator $streamEvents)
    {
        try {
            foreach ($streamEvents as $event) {
                $this->insertEvent($streamName, $event);
            }
        } catch (UniqueConstraintViolationException $e) {
            throw new ConcurrencyException('At least one event with same version exists already', 0, $e);
        }
    }

    /**
     * Insert an event
     *
     * @param StreamName $streamName
     * @param Message $e
     * @return void
     */
    private function insertEvent(StreamName $streamName, Message $e)
    {
        $eventArr = $this->messageConverter->convertToArray($e);

        MessageDataAssertion::assert($eventArr);

        $eventData = [
            'event_id' => $eventArr['uuid'],
            'version' => $eventArr['version'],
            'event_name' => $eventArr['message_name'],
            'payload' => $this->payloadSerializer->serializePayload($eventArr['payload']),
            'created_at' => $eventArr['created_at']->format('Y-m-d\TH:i:s.u'),
        ];

        foreach ($eventArr['metadata'] as $key => $value) {
            $eventData[$key] = (string)$value;
        }

        $this->connection->insert($this->getTable($streamName), $eventData);
    }

    /**
     * @param StreamName $streamName
     * @param DateTimeInterface|null $since
     * @param array $metadata
     * @return CakePhpOrmStreamIterator
     */
    public function replay(StreamName $streamName, \DateTimeInterface $since = null, array $metadata = [])
    {
        $queryBuilder = $this->createQueryBuilder($streamName, $metadata);
        $table = $this->getTable($streamName);

        $queryBuilder
            ->select()
            ->from($table)
            ->orderAsc('created_at')
            ->orderAsc('version');

        foreach ($metadata as $key => $value) {
            $queryBuilder->andWhere([
                $table . '.' . $key => (string)$value
            ]);
        }

        if (null !== $since) {
            $queryBuilder->andWhere([
                'created_at >' => $since->format('Y-m-d\TH:i:s.u')
            ]);
        }

        return new CakePhpOrmStreamIterator(
            $queryBuilder,
            $this->messageFactory,
            $this->payloadSerializer,
            $metadata,
            $this->loadBatchSize
        );
    }

    /**
     * @param StreamName $streamName
     * @param array $metadata
     * @param null $minVersion
     * @return CakePhpOrmStreamIterator
     */
    public function loadEvents(StreamName $streamName, array $metadata = [], $minVersion = null)
    {
        $queryBuilder = $this->createQueryBuilder($streamName, $metadata);
        $table = $this->getTable($streamName);
        $queryBuilder
            ->select()
            ->from($table)
            ->orderAsc($table.'.version');
        foreach ($metadata as $key => $value) {
            $queryBuilder->andWhere([
                $table . '.' . $key => (string)$value
            ]);
        }
        if (null !== $minVersion) {
            $queryBuilder->andWhere([
                $table . '.version >=' => $minVersion
            ]);
        }
        return new CakePhpOrmStreamIterator(
            $queryBuilder,
            $this->messageFactory,
            $this->payloadSerializer,
            $metadata,
            $this->loadBatchSize
        );

    }

    /**
     * @param StreamName $streamName
     * @param $metadata
     * @return Query
     */
    private function createQueryBuilder(StreamName $streamName, $metadata)
    {
        $table = $this->getTable($streamName);
        $tableSchema = new TableSchema($table);
        static::addToTableSchema($tableSchema, $metadata);
        return new Query(
            $this->connection,
            new Table(
                [
                    'alias' => null,
                    'table' => $table,
                    'schema' => $tableSchema,
                    'connection' => $this->connection
                ]
            )
        );
    }
}
